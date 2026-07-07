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
git clone https://github.com/vjeromeadamcote/verily-raw-data-tools.git
cd verily-raw-data-tools
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

Use the sensor-specific PTransforms to unpack compressed data into time series:

```python
from verily.raw_data_tools import RawDataIO
from verily.raw_data_tools.unpacking import UnpackImu

io = RawDataIO(project='my-project', dataset='sensors', runner='DirectRunner')
pipeline = io.create_pipeline()

# Read compressed sensor data
compressed_data = pipeline | io.read_datapoints(
    data_types=['IMU'],
    start_time='2024-01-01',
    end_time='2024-01-02'
)

# Unpack into time series
unpacked_data = compressed_data | 'Unpack' >> UnpackImu()

pipeline.run().wait_until_finish()
```

Available unpackers: `UnpackImu`, `UnpackPpg`, `UnpackEcg`, `UnpackEda`,
`UnpackTwoChannelPpg`, `UnpackPicardEda`.

### Segmenting Data with Key-By

```python
from verily.raw_data_tools.transforms import KeyBy, BuildDataFrames
from verily.raw_data_tools.unpacking import UnpackPpg

# ... (after reading and unpacking data) ...

# Key by device ID
by_device = unpacked_data | 'Key by Device' >> KeyBy(key_field='DeviceID')

# Key by participant ID
by_participant = unpacked_data | 'Key by Participant' >> KeyBy(key_field='ParticipantID')

# Key by both device and participant
by_both = unpacked_data | 'Key by Both' >> KeyBy(key_field='Both')

# Build DataFrames from keyed data
dataframes = by_device | 'Build DFs' >> BuildDataFrames()
```

### Integrating Custom Algorithms

```python
from verily.raw_data_tools import apply_to_dataframe
import pandas as pd

# Define your custom processing function
def calculate_heart_rate(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate heart rate from PPG signal."""
    df['heart_rate'] = df['ppg_value'].rolling(window=100).mean()
    return df

# Apply to keyed DataFrames in the pipeline
processed = dataframes | apply_to_dataframe(calculate_heart_rate)
```

### Running on Dataflow

```python
from verily.raw_data_tools import RawDataIO, DataflowOptions

# Configure Dataflow
dataflow_opts = DataflowOptions(
    job_name='sensor-processing-job',
    region='us-central1',
    additional_options={
        'temp_location': 'gs://my-bucket/temp',
        'staging_location': 'gs://my-bucket/staging',
        'num_workers': 10,
        'max_num_workers': 50,
        'machine_type': 'n1-standard-4',
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
```

## Core Capabilities

### 1. Data Reading

The `RawDataIO` class provides the main interface for reading sensor data from BigQuery.

**Key Features:**
- Query by device ID, participant ID, time range, and data type
- Automatic query construction with SQL injection prevention
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

Sensor-specific PTransforms unpack compressed data packets into time-series DataPoints.

**Key Features:**
- Handles variable sampling rates
- Automatic timestamp reconstruction
- Support for IMU, PPG, ECG, EDA, and other sensor types
- Numba-accelerated for performance

**Example:**
```python
from verily.raw_data_tools.unpacking import UnpackImu, UnpackPpg

imu_unpacked = compressed_imu | 'Unpack IMU' >> UnpackImu()
ppg_unpacked = compressed_ppg | 'Unpack PPG' >> UnpackPpg()
```

### 3. DataFrame Transforms

Transform DataPoints into structured Pandas DataFrames for analysis.

**Available Transforms:**
- `BuildDataFrames`: Build DataFrames from keyed DataPoints
- `GroupIntoDataFrames`: Lower-level grouping and aggregation

**Example:**
```python
from verily.raw_data_tools.transforms import BuildDataFrames

# Convert keyed data to DataFrames
dfs = keyed_data | 'Build DFs' >> BuildDataFrames()

# With time windowing (group into 5-minute windows)
windowed_dfs = keyed_data | 'Windowed DFs' >> BuildDataFrames(
    window_seconds=300
)
```

### 4. Data Segmentation (Key-By)

Segment data by device ID, participant ID, or both.

**Valid key fields:** `'DeviceID'`, `'ParticipantID'`, `'Both'`

**Example:**
```python
from verily.raw_data_tools.transforms import KeyBy

# Key by device
by_device = data | KeyBy(key_field='DeviceID')

# Key by participant
by_participant = data | KeyBy(key_field='ParticipantID')

# Key by both device and participant
by_both = data | KeyBy(key_field='Both')
```

**Time-Based Windowing** can be applied using standard Beam transforms:
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

**A. Simple Function Mapping:**
```python
from verily.raw_data_tools.transforms import apply_to_dataframe

def my_algorithm(df: pd.DataFrame) -> pd.DataFrame:
    df['result'] = df['value'].rolling(100).mean()
    return df

processed = dataframes | apply_to_dataframe(my_algorithm)
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

processed = dataframes | apply_algorithm(
    MyAnalyzer,
    method_name='analyze',
    init_args={'threshold': 95.0}
)
```

**C. Advanced - Custom Beam Transform:**
```python
from verily.raw_data_tools.transforms import MapWithCustomFunction

processed = dataframes | MapWithCustomFunction(my_function)
```

### 6. Pipeline Building & Launching

Build and deploy production pipelines on Google Cloud Dataflow.

**Dataflow Configuration:**
```python
from verily.raw_data_tools import DataflowOptions

opts = DataflowOptions(
    job_name='my-sensor-pipeline',
    region='us-central1',
    additional_options={
        'temp_location': 'gs://my-bucket/temp',
        'staging_location': 'gs://my-bucket/staging',
        'num_workers': 20,
        'max_num_workers': 100,
        'machine_type': 'n1-highmem-4',
        'disk_size_gb': 100,
        'use_public_ips': False,
        'experiments': ['use_runner_v2'],
    }
)
```

## Examples

See the `examples/` directory for complete working examples:

- `examples/basic_data_read.py` - Simple data reading from BigQuery
- `examples/unpack_and_transform.py` - Unpacking and DataFrame transformation
- `examples/custom_algorithm.py` - Integrating a custom heart rate algorithm
- `examples/full_pipeline.py` - End-to-end production pipeline on Dataflow

All examples run a synthetic-data demo when `GOOGLE_PROJECT` is not set.

## Workbench Integration

### Authentication

In Verily Workbench, authentication to BigQuery is handled automatically using your Workbench credentials. No additional setup required!

### Storage

Store pipeline artifacts (temp files, staging) in Workbench-managed GCS buckets:

```python
import os
from verily.raw_data_tools import DataflowOptions

dataflow_opts = DataflowOptions(
    job_name='my-pipeline',
    additional_options={
        'temp_location': f"gs://{os.environ['WORKSPACE_BUCKET']}/temp",
        'staging_location': f"gs://{os.environ['WORKSPACE_BUCKET']}/staging",
    }
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
- `get_table_schema(table: str) -> List[bigquery.SchemaField]`
- `list_tables() -> List[str]`

### UnpackImu / UnpackPpg / UnpackEcg / UnpackEda

Sensor-specific PTransforms for unpacking compressed data. Use these instead of
`DataUnpacker` directly.

```python
from verily.raw_data_tools.unpacking import UnpackImu, UnpackPpg

unpacked_imu = compressed_data | UnpackImu()
unpacked_ppg = compressed_data | UnpackPpg()
```

### KeyBy

Segment data by key fields.

```python
KeyBy(
    key_field: str = 'DeviceID'  # One of 'DeviceID', 'ParticipantID', 'Both'
)
```

### BuildDataFrames

Convert keyed DataPoints to DataFrames.

```python
BuildDataFrames(
    window_seconds: Optional[int] = None,   # Fixed time window size in seconds
    combine_method: Optional[str] = None     # How to combine multiple sources
)
```

### DataflowOptions

Configuration for Dataflow pipelines.

```python
DataflowOptions(
    job_name: str,                                   # Dataflow job name
    region: str = 'us-central1',                     # GCP region
    additional_options: Dict[str, Any] = {}           # All other Dataflow options
)
```

Runner-specific options (`temp_location`, `staging_location`, `num_workers`,
`max_num_workers`, `machine_type`, `disk_size_gb`, etc.) go in
`additional_options`.

### apply_to_dataframe

```python
apply_to_dataframe(
    func: Callable[[pd.DataFrame], pd.DataFrame],
    label: Optional[str] = None
) -> MapWithCustomFunction
```

### apply_algorithm

```python
apply_algorithm(
    algorithm_class: type,
    method_name: str = 'process',
    init_args: Optional[dict] = None,
    label: Optional[str] = None
) -> MapWithCustomFunction
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
```

### 3. Batch Processing
Process data in batches for better throughput:
```python
batched = data | beam.BatchElements(
    min_batch_size=100,
    max_batch_size=1000
)
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
    job_name='my-pipeline',
    additional_options={
        'machine_type': 'n1-highmem-8',  # More memory
        'disk_size_gb': 200,              # More disk
    }
)
```

## Contributing

This project is maintained by Verily Life Sciences. For bug reports or feature requests, please open an issue on GitHub.

## License

Apache License 2.0

## Support

For Workbench-specific questions: workbench-support@verily.com

For SDK issues: https://github.com/vjeromeadamcote/verily-raw-data-tools/issues
