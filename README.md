# Verily Raw Data Tools

**Version 1.0.0**

Tools for reading, unpacking, and transforming sensor data from BigQuery in Verily Workbench.

## Overview

Verily Raw Data Tools is a Python SDK designed for processing sensor data stored in BigQuery. It provides a streamlined interface for Workbench users to:

1. **Read Data** from BigQuery with filtering, deduplication, and joins
2. **Unpack** compressed sensor data into time-series format
3. **Transform** DataPoints into Pandas DataFrames
4. **Segment** data by device ID, participant ID, or time windows
5. **Integrate** 3rd-party algorithms and custom Python functions
6. **Build & Launch** Dataflow pipelines for large-scale processing

## Installation

### In Verily Workbench

```bash
pip install verily-raw-data-tools
```

### Local Development

```bash
git clone https://github.com/verily-src/sensorsuite-ds-sdk.git
cd sensorsuite-ds-sdk
pip install -e .
```

## Quick Start

### Basic Data Reading

```python
from verily.raw_data_tools import RawDataIO

# Initialize I/O
io = RawDataIO(
    project='my-gcp-project',
    dataset='my_sensor_dataset',
    runner='DirectRunner'  # or 'DataflowRunner' for production
)

# Create a pipeline
pipeline = io.create_pipeline()

# Read DataPoints from BigQuery
data = pipeline | io.read_datapoints(
    device_ids=['device_001', 'device_002'],
    start_time='2024-01-01',
    end_time='2024-01-31',
    data_types=['IMU', 'PPG']
)

# Run the pipeline
result = pipeline.run()
result.wait_until_finish()
```

### Unpacking Sensor Data

```python
from verily.raw_data_tools import RawDataIO, DataUnpacker
import apache_beam as beam

io = RawDataIO(project='my-project', dataset='sensors', runner='DirectRunner')
pipeline = io.create_pipeline()

# Read compressed sensor data
compressed_data = pipeline | io.read_datapoints(
    data_types=['IMU'],
    start_time='2024-01-01',
    end_time='2024-01-02'
)

# Unpack into time series
unpacker = DataUnpacker()
unpacked_data = compressed_data | 'Unpack' >> beam.ParDo(unpacker)

# Convert to DataFrames
from verily.raw_data_tools.transforms import BuildDataFrames

dataframes = unpacked_data | BuildDataFrames()

pipeline.run().wait_until_finish()
```

### Segmenting Data with Key-By

```python
from verily.raw_data_tools import RawDataIO, KeyBy
import apache_beam as beam

io = RawDataIO(project='my-project', dataset='sensors', runner='DirectRunner')
pipeline = io.create_pipeline()

data = pipeline | io.read_datapoints(data_types=['PPG'])

# Key by device ID
by_device = data | 'Key by Device' >> KeyBy(key_field='DeviceID')

# Key by participant ID
by_participant = data | 'Key by Participant' >> KeyBy(key_field='ParticipantID')

# Key by time windows (5-minute windows)
from apache_beam.transforms.window import FixedWindows
from apache_beam import WindowInto
import apache_beam.transforms.window as window

windowed_data = data | WindowInto(window.FixedWindows(5 * 60))  # 5 minutes in seconds

pipeline.run().wait_until_finish()
```

### Integrating Custom Algorithms

```python
from verily.raw_data_tools import RawDataIO, apply_to_dataframe
import apache_beam as beam
import pandas as pd

# Define your custom processing function
def calculate_heart_rate(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate heart rate from PPG signal."""
    # Your algorithm here
    df['heart_rate'] = df['ppg_value'].rolling(window=100).apply(
        lambda x: detect_peaks(x) * 60
    )
    return df

# Integrate into pipeline
io = RawDataIO(project='my-project', dataset='sensors', runner='DirectRunner')
pipeline = io.create_pipeline()

data = pipeline | io.read_datapoints(data_types=['PPG'])

# Apply custom algorithm
processed = data | apply_to_dataframe(calculate_heart_rate)

pipeline.run().wait_until_finish()
```

### Running on Dataflow

```python
from verily.raw_data_tools import RawDataIO, DataflowOptions

# Configure Dataflow
dataflow_opts = DataflowOptions(
    job_name='sensor-processing-job',
    temp_location='gs://my-bucket/temp',
    staging_location='gs://my-bucket/staging',
    region='us-central1',
    num_workers=10,
    max_num_workers=50,
    machine_type='n1-standard-4',
    additional_options={
        'subnetwork': 'regions/us-central1/subnetworks/my-subnet'
    }
)

io = RawDataIO(
    project='my-project',
    dataset='sensors',
    runner='DataflowRunner',
    dataflow_options=dataflow_opts
)

pipeline = io.create_pipeline()

# Build your pipeline
data = pipeline | io.read_datapoints(
    data_types=['IMU', 'PPG'],
    start_time='2024-01-01',
    end_time='2024-12-31'
)

# ... add transforms ...

# Launch on Dataflow
result = pipeline.run()
print(f"Dataflow job started. Monitor at: {result.metrics()}")
```

## Core Capabilities

### 1. Data Reading

The `RawDataIO` class provides the main interface for reading sensor data from BigQuery.

**Key Features:**
- Query by device ID, participant ID, time range, and data type
- Automatic query construction
- Support for both DirectRunner (local) and DataflowRunner (cloud)
- Built-in BigQuery authentication via Workbench

**Example:**
```python
io = RawDataIO(project='my-project', dataset='sensors')

# Read with multiple filters
data = pipeline | io.read_datapoints(
    device_ids=['dev1', 'dev2'],
    start_time='2024-01-01T00:00:00',
    end_time='2024-01-31T23:59:59',
    data_types=['IMU', 'PPG', 'ECG'],
    limit=10000  # Optional row limit
)
```

### 2. Data Unpacking

The `DataUnpacker` class unpacks compressed sensor data packets into time-series DataFrames.

**Key Features:**
- Handles variable sampling rates
- Automatic timestamp reconstruction
- Support for IMU, PPG, ECG, and other sensor types
- Numba-accelerated for performance

**Example:**
```python
from verily.raw_data_tools.unpacking import DataUnpacker

unpacker = DataUnpacker(
    error_thresh=0.05,  # Sampling rate error threshold
    ignore_median_fs_error=False
)

unpacked = compressed_data | 'Unpack' >> beam.ParDo(unpacker)
```

### 3. DataFrame Transforms

Transform DataPoints into structured Pandas DataFrames for analysis.

**Available Transforms:**
- `BuildDataFrames`: Build DataFrames from DataPoints
- `GroupIntoDataFrames`: Group and aggregate DataPoints into DataFrames

**Example:**
```python
from verily.raw_data_tools.transforms import BuildDataFrames

# Convert to DataFrames
dfs = data | 'Build DFs' >> BuildDataFrames(
    include_metadata=True,
    sort_by_time=True
)
```

### 4. Data Segmentation (Key-By)

Segment data by various keys for parallel processing.

**Key Types:**
- Device ID
- Participant ID
- Session ID
- Time windows (fixed, sliding, session-based)

**Example:**
```python
from verily.raw_data_tools.transforms import KeyBy

# Key by device
by_device = data | KeyBy(key_field='DeviceID')

# Key by participant
by_participant = data | KeyBy(key_field='ParticipantID')

# Key by custom field
by_session = data | KeyBy(key_field='SessionID')
```

**Time-Based Windowing:**
```python
import apache_beam as beam
from apache_beam.transforms.window import FixedWindows, SlidingWindows

# Fixed 10-minute windows
fixed_windows = data | beam.WindowInto(FixedWindows(10 * 60))

# Sliding windows (10-min window, 5-min slide)
sliding_windows = data | beam.WindowInto(
    SlidingWindows(size=10 * 60, period=5 * 60)
)
```

### 5. Custom Algorithm Integration

Integrate your own Python functions or 3rd-party algorithms into the pipeline.

**Three Ways to Integrate:**

**A. Simple Function Mapping:**
```python
from verily.raw_data_tools.transforms import apply_to_dataframe

def my_algorithm(df: pd.DataFrame) -> pd.DataFrame:
    df['result'] = df['value'].rolling(100).mean()
    return df

processed = data | apply_to_dataframe(my_algorithm)
```

**B. Custom Class Integration:**
```python
from verily.raw_data_tools.transforms import apply_algorithm

class MyAnalyzer:
    def __init__(self, threshold: float):
        self.threshold = threshold

    def analyze(self, df: pd.DataFrame) -> pd.DataFrame:
        df['anomaly'] = df['value'] > self.threshold
        return df

processed = data | apply_algorithm(
    MyAnalyzer,
    method_name='analyze',
    init_args={'threshold': 95.0}
)
```

**C. Advanced - Custom Beam Transform:**
```python
from verily.raw_data_tools.transforms import MapWithCustomFunction

def process_with_context(element, threshold, window_size):
    # Your complex processing logic
    return processed_element

processed = data | MapWithCustomFunction(
    process_with_context,
    threshold=90,
    window_size=1000
)
```

### 6. Pipeline Building & Launching

Build and deploy production pipelines on Google Cloud Dataflow.

**Dataflow Configuration:**
```python
from verily.raw_data_tools.pipeline import DataflowOptions

opts = DataflowOptions(
    job_name='my-sensor-pipeline',
    temp_location='gs://my-bucket/temp',
    staging_location='gs://my-bucket/staging',
    region='us-central1',
    num_workers=20,
    max_num_workers=100,
    machine_type='n1-highmem-4',
    disk_size_gb=100,
    use_public_ips=False,
    additional_options={
        'experiments': ['use_runner_v2'],
        'subnetwork': 'regions/us-central1/subnetworks/workbench-subnet'
    }
)
```

**Docker Image Building** (for custom dependencies):
```python
from verily.raw_data_tools.pipeline.docker import WorkerImage

# Build custom worker image with your dependencies
image_builder = WorkerImage(
    project='my-project',
    requirements_file='requirements.txt',
    dockerfile='Dockerfile.worker'
)

image_uri = image_builder.build_and_push(
    tag='my-pipeline-v1'
)

# Use in Dataflow options
opts = DataflowOptions(
    worker_harness_container_image=image_uri,
    # ... other options
)
```

## Examples

See the `examples/` directory for complete working examples:

- `examples/basic_data_read.py` - Simple data reading from BigQuery
- `examples/unpack_and_transform.py` - Unpacking and DataFrame transformation
- `examples/custom_algorithm.py` - Integrating a custom heart rate algorithm
- `examples/full_pipeline.py` - End-to-end production pipeline on Dataflow

## Workbench Integration

### Authentication

In Verily Workbench, authentication to BigQuery is handled automatically using your Workbench credentials. No additional setup required!

### Storage

Store pipeline artifacts (temp files, staging) in Workbench-managed GCS buckets:

```python
# Workbench provides environment variables
import os

dataflow_opts = DataflowOptions(
    temp_location=f"gs://{os.environ['WORKSPACE_BUCKET']}/temp",
    staging_location=f"gs://{os.environ['WORKSPACE_BUCKET']}/staging",
)
```

### Data Collections

Access Data Collections (curated BigQuery datasets) directly:

```python
io = RawDataIO(
    project=os.environ['GOOGLE_PROJECT'],
    dataset='my_data_collection',  # Your Data Collection name
    runner='DirectRunner'
)
```

## API Reference

### RawDataIO

Main class for I/O operations.

```python
RawDataIO(
    project: str,                    # GCP project ID
    dataset: str,                    # BigQuery dataset name
    runner: str = 'DirectRunner',    # Beam runner
    dataflow_options: Optional[DataflowOptions] = None,
    bigquery_location: str = 'US'    # BQ dataset location
)
```

**Methods:**
- `create_pipeline(name: Optional[str] = None) -> beam.Pipeline`
- `read_datapoints(...) -> beam.PTransform`
- `get_table_schema(table: str) -> bigquery.Schema`
- `list_tables() -> List[str]`

### DataUnpacker

Unpacks compressed sensor data.

```python
DataUnpacker(
    error_thresh: float = 0.05,           # Sampling rate error threshold
    ignore_median_fs_error: bool = False,  # Ignore median FS errors
    fallback_to_legacy: bool = False       # Use legacy unpacking
)
```

### KeyBy

Segment data by key fields.

```python
KeyBy(
    key_field: str,           # Field name to key by ('DeviceID', 'ParticipantID', etc.)
    preserve_metadata: bool = True
)
```

### BuildDataFrames

Convert DataPoints to DataFrames.

```python
BuildDataFrames(
    include_metadata: bool = True,  # Include DataPoint metadata in DataFrame
    sort_by_time: bool = True,       # Sort by timestamp
    deduplicate: bool = False        # Remove duplicate timestamps
)
```

### DataflowOptions

Configuration for Dataflow pipelines.

```python
DataflowOptions(
    job_name: str,
    temp_location: str,          # gs:// path
    staging_location: str,       # gs:// path
    region: str = 'us-central1',
    num_workers: int = 1,
    max_num_workers: int = 10,
    machine_type: str = 'n1-standard-4',
    disk_size_gb: int = 50,
    use_public_ips: bool = True,
    service_account: Optional[str] = None,
    additional_options: Optional[Dict] = None
)
```

## Performance Tips

### 1. Use Dataflow for Large Datasets
For datasets > 10GB, use DataflowRunner instead of DirectRunner.

### 2. Optimize BigQuery Queries
Filter as much as possible in the `read_datapoints()` call:
```python
# Good - filters applied in BigQuery
data = io.read_datapoints(
    device_ids=['dev1', 'dev2'],
    start_time='2024-01-01',
    data_types=['IMU']
)

# Less efficient - reads everything then filters
data = io.read_datapoints()  # Reads all data
filtered = data | beam.Filter(lambda x: x['DeviceID'] in ['dev1', 'dev2'])
```

### 3. Batch Processing
Process data in batches for better throughput:
```python
batched = data | beam.BatchElements(
    min_batch_size=100,
    max_batch_size=1000
)
```

### 4. Use Numba Unpacking
For IMU/PPG data, Numba unpacking is significantly faster:
```python
unpacker = DataUnpacker(fallback_to_legacy=False)  # Uses Numba by default
```

## Troubleshooting

### "Permission denied" errors
Ensure your Workbench environment has access to the BigQuery dataset.

### "Module not found" errors
Rebuild your requirements file and Docker image if using custom dependencies.

### Dataflow job fails
Check Dataflow logs in GCP Console:
```
https://console.cloud.google.com/dataflow/jobs
```

### Out of memory errors
Increase machine type or reduce batch size:
```python
dataflow_opts = DataflowOptions(
    machine_type='n1-highmem-8',  # More memory
    disk_size_gb=200               # More disk
)
```

## Contributing

This project is maintained by Verily Life Sciences. For bug reports or feature requests, please open an issue on GitHub.

## License

Apache License 2.0

## Support

For Workbench-specific questions: workbench-support@verily.com

For SDK issues: https://github.com/verily-src/sensorsuite-ds-sdk/issues
