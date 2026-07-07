# Verily Raw Data Tools v1.0.0 - Release Notes

**Release Date**: June 11, 2026  
**Package Name**: `verily-raw-data-tools`  
**Previous Package**: `verily-ds-sdk` (v6.13.0 - now deprecated)

## 🎉 What's New

This is the first external release of Verily's sensor data processing tools, rebranded as **Verily Raw Data Tools** for use in Verily Workbench and external environments.

### New Package Structure

```
verily-raw-data-tools/
├── verily/raw_data_tools/
│   ├── io/                    # Data reading and filtering
│   ├── unpacking/             # Sensor data unpacking
│   ├── transforms/            # DataFrames, key-by, custom algorithms
│   ├── pipeline/              # Dataflow pipeline building
│   ├── schemas/               # Data schemas
│   └── utils/                 # Utility functions
├── examples/                  # Complete working examples
└── docs/                      # Documentation
```

## ✨ Core Features

### 1. Simplified Data Reading
- **New Class**: `RawDataIO` - Streamlined interface for BigQuery data access
- No more internal registries or environments
- Direct project/dataset configuration
- Automatic Workbench authentication

```python
from verily.raw_data_tools import RawDataIO

io = RawDataIO(
    project='my-project',
    dataset='my_dataset',
    runner='DirectRunner'
)

pipeline = io.create_pipeline()
data = pipeline | io.read_datapoints(
    device_ids=['device1'],
    start_time='2024-01-01',
    end_time='2024-01-31',
    data_types=['IMU', 'PPG']
)
```

### 2. Data Unpacking
- Unpack compressed sensor data into time series
- Numba-accelerated for performance
- Support for IMU, PPG, ECG, and other sensor types
- Configurable sampling rate error handling

### 3. DataFrame Transforms
- Convert DataPoints to Pandas DataFrames
- Group and aggregate sensor data
- Time-based sorting and deduplication

### 4. Data Segmentation (Key-By)
- Key data by device ID, participant ID, or custom fields
- Time window support (fixed, sliding, session-based)
- Parallel processing optimization

### 5. Custom Algorithm Integration (NEW!)
- **`apply_to_dataframe`**: Apply custom functions to DataFrames
- **`apply_algorithm`**: Integrate 3rd-party algorithm classes
- **`MapWithCustomFunction`**: Beam transform wrapper for custom code
- Easy integration of ML models, signal processing, or domain-specific algorithms

```python
from verily.raw_data_tools import apply_to_dataframe

def my_algorithm(df: pd.DataFrame) -> pd.DataFrame:
    # Your custom processing
    df['result'] = df['value'].rolling(100).mean()
    return df

processed = data | apply_to_dataframe(my_algorithm)
```

### 6. Pipeline Building & Launching
- Simplified Dataflow configuration
- Docker image building for custom dependencies
- Workbench-optimized defaults
- Production-ready pipeline templates

## 🚀 Improvements Over DS SDK

### Removed Internal Dependencies
- ❌ SensorStore client (internal Verily infrastructure)
- ❌ gRPC registry service (internal)
- ❌ Internal authentication systems
- ❌ Study management complexity
- ❌ Environment-based configuration

### Simplified API
- ✅ 60% fewer required parameters
- ✅ Direct BigQuery access without abstractions
- ✅ Standard Google Cloud authentication
- ✅ Clearer import paths
- ✅ Better error messages

### Enhanced Documentation
- ✅ Comprehensive README with examples
- ✅ Migration guide from DS SDK
- ✅ 4 complete working examples
- ✅ API reference documentation
- ✅ Troubleshooting guide

## 📦 Installation

### PyPI (recommended for Workbench)
```bash
pip install verily-raw-data-tools
```

### From Source
```bash
git clone https://github.com/vjeromeadamcote/verily-raw-data-tools.git
cd verily-raw-data-tools
pip install -e .
```

## 📚 Documentation

- **README**: `README.md` - Complete usage guide
- **Migration Guide**: `MIGRATION_GUIDE.md` - Migrate from DS SDK
- **Externalization Plan**: `EXTERNALIZATION_PLAN.md` - Design decisions
- **Examples**: `examples/` directory - Working code samples

## 🔧 Requirements

- Python 3.11+
- Apache Beam 2.43.0+
- Google Cloud BigQuery access
- For Dataflow: GCS bucket access

## 📝 Examples Included

### 1. `basic_data_read.py`
Simple example of reading sensor data from BigQuery with filters.

### 2. `unpack_and_transform.py`
Unpacking compressed IMU data and converting to DataFrames.

### 3. `custom_algorithm.py`
Integrating a custom heart rate detection algorithm into the pipeline.

### 4. `full_pipeline.py`
Complete end-to-end pipeline on Dataflow with custom processing and BigQuery output.

## 🔄 Migration from DS SDK

If you're currently using `verily-ds-sdk`, see `MIGRATION_GUIDE.md` for:
- Import path changes
- API equivalents
- Common migration issues
- Step-by-step checklist

**Key Changes**:
```python
# Old (DS SDK)
from verily.ds_sdk.core import sensors_io
io = sensors_io.SensorsIO(registry='my-registry', env='prod', ...)

# New (Raw Data Tools)
from verily.raw_data_tools import RawDataIO
io = RawDataIO(project='my-project', dataset='my_dataset', ...)
```

## ⚠️ Breaking Changes

This is a new package, not a drop-in replacement for DS SDK:

1. **Package name changed**: `verily-ds-sdk` → `verily-raw-data-tools`
2. **Import paths changed**: `verily.ds_sdk.*` → `verily.raw_data_tools.*`
3. **Main class renamed**: `SensorsIO` → `RawDataIO`
4. **Method renamed**: `echo_data_point_rows()` → `read_datapoints()`
5. **Parameters changed**: Direct params instead of Condition objects
6. **Removed features**: Internal-only features (see above)

## 🐛 Known Issues

- BigQuery queries with >1M rows may require optimization
- Docker image building requires GCP permissions
- Time zone handling assumes UTC for timestamp conversions

## 🛣️ Roadmap

### v1.1.0 (Q3 2026)
- Add PubSub streaming support
- Enhanced windowing utilities
- Performance optimizations for large datasets

### v1.2.0 (Q4 2026)
- Machine learning integration helpers
- Visualization utilities
- Enhanced error handling and logging

## 🙏 Acknowledgments

Built by the Verily Sensors Infrastructure team, externalized for Workbench customers.

Special thanks to all DS SDK contributors whose work formed the foundation of this release.

## 📧 Support

- **Bug Reports**: https://github.com/vjeromeadamcote/verily-raw-data-tools/issues
- **Workbench Questions**: workbench-support@verily.com
- **Documentation**: https://github.com/vjeromeadamcote/verily-raw-data-tools

## 📄 License

Apache License 2.0

---

**Note**: The legacy `verily-ds-sdk` package (v6.13.0) is now deprecated and will reach end-of-support in Q3 2026. Please migrate to `verily-raw-data-tools` as soon as possible.
