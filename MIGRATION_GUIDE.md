# Migration Guide: DS SDK → Raw Data Tools

## Overview

This guide helps you migrate from the deprecated `verily-ds-sdk` to the new `verily-raw-data-tools`.

## Key Changes

### Package Name
- **Old**: `verily-ds-sdk`
- **New**: `verily-raw-data-tools`

### Import Paths
- **Old**: `from verily.ds_sdk.core import ...`
- **New**: `from verily.raw_data_tools import ...`

### Version
- **Old**: 6.13.0 (internal only)
- **New**: 1.0.0 (external release)

## What's Different

### Removed Features
The following internal-only features have been removed:

1. **SensorStore Integration** - Direct writes to internal SensorStore
2. **gRPC Registry Service** - Internal registry lookups
3. **PubSub Streaming** - Will be added in future release
4. **Internal Authentication** - Use standard GCP auth instead
5. **Study Management** - Simplified to project/dataset config

### Simplified API

#### Old Way (DS SDK)
```python
from verily.ds_sdk.core import sensors_io, options, studies

study = studies.get_study_info('my-study')
io = sensors_io.SensorsIO(
    registry='my-registry',
    runner='DataflowRunner',
    env='prod',
    study_info=study
)
```

#### New Way (Raw Data Tools)
```python
from verily.raw_data_tools import RawDataIO, DataflowOptions

io = RawDataIO(
    project='my-gcp-project',
    dataset='my_dataset',
    runner='DataflowRunner'
)
```

### Authentication

#### Old Way
```python
from verily.ds_sdk.core.gcp import credentials

creds = credentials.DsSdkCredentials(
    service_account='internal-sa@verily.com'
)
```

#### New Way
In Workbench, authentication is automatic. Locally, use standard Google Cloud auth:
```bash
gcloud auth application-default login
```

## Migration Examples

### Example 1: Reading Data

#### Old Code
```python
from verily.ds_sdk.core import sensors_io, conditions

io = sensors_io.SensorsIO(
    registry='my-registry',
    runner='DirectRunner',
    env='prod'
)

pipeline = io.create_pipeline()
data = pipeline | io.echo_data_point_rows(
    conditions=[
        conditions.DeviceIdCondition(['device1', 'device2']),
        conditions.TimeRangeCondition('2024-01-01', '2024-01-31'),
    ]
)
```

#### New Code
```python
from verily.raw_data_tools import RawDataIO

io = RawDataIO(
    project='my-project',
    dataset='my_dataset',
    runner='DirectRunner'
)

pipeline = io.create_pipeline()
data = pipeline | io.read_datapoints(
    device_ids=['device1', 'device2'],
    start_time='2024-01-01',
    end_time='2024-01-31'
)
```

### Example 2: Unpacking Data

#### Old Code
```python
from verily.ds_sdk.contrib import data_unpacking

unpacker = data_unpacking.DataUnpacker()
unpacked = compressed_data | beam.ParDo(unpacker)
```

#### New Code
```python
from verily.raw_data_tools.unpacking import UnpackImu

unpacked = compressed_data | UnpackImu()
```

### Example 3: Transforms

#### Old Code
```python
from verily.ds_sdk.core.transforms.atomic import key_by
from verily.ds_sdk.core.transforms.composite import build_data_frames

keyed = data | key_by.KeyBy(key_field='DeviceID')
dfs = keyed | build_data_frames.BuildDataFrames()
```

#### New Code
```python
from verily.raw_data_tools.transforms import KeyBy, BuildDataFrames

keyed = data | KeyBy(key_field='DeviceID')
dfs = keyed | BuildDataFrames()
```

### Example 4: Dataflow Pipeline

#### Old Code
```python
from verily.ds_sdk.core import options

dataflow_opts = options.DataflowOptions(
    job_name='my-job',
    gcp_project='my-project',
    temp_bucket='my-bucket',
    # ... many internal options
)

io = sensors_io.SensorsIO(
    registry='my-registry',
    runner='DataflowRunner',
    env='prod',
    dataflow_options=dataflow_opts
)
```

#### New Code
```python
from verily.raw_data_tools import RawDataIO, DataflowOptions

dataflow_opts = DataflowOptions(
    job_name='my-job',
    region='us-central1',
    additional_options={
        'temp_location': 'gs://my-bucket/temp',
        'staging_location': 'gs://my-bucket/staging',
    }
)

io = RawDataIO(
    project='my-project',
    dataset='my_dataset',
    runner='DataflowRunner',
    dataflow_options=dataflow_opts
)
```

## Feature Mapping

| DS SDK Feature | Raw Data Tools Equivalent | Status |
|----------------|---------------------------|--------|
| `sensors_io.SensorsIO` | `RawDataIO` | ✅ Available |
| `echo_data_point_rows()` | `read_datapoints()` | ✅ Available |
| `data_unpacking.DataUnpacker` | `DataUnpacker` | ✅ Available |
| `key_by.KeyBy` | `KeyBy` | ✅ Available |
| `build_data_frames.BuildDataFrames` | `BuildDataFrames` | ✅ Available |
| `DataflowOptions` | `DataflowOptions` | ✅ Available |
| `worker_image.WorkerImage` | `WorkerImage` | ✅ Available |
| SensorStore writes | N/A | ❌ Removed |
| PubSub streaming | N/A | 🔄 Future |
| Registry lookups | Direct BQ access | ✅ Simplified |
| Internal auth | Standard GCP auth | ✅ Simplified |

## v1.0 Limitations

### Timezone-Aware KeyBy Transforms
The DS SDK's `KeyDataPointsByParticipantDeviceTimeRangeInLocalTimezone`
transform requires `DataSourceCache` for device-to-timezone mappings.
In v1.0 the `DataSourceCache` is a stub that raises `NotImplementedError`
when queried — this transform will fail at runtime. Use the non-timezone-aware
variants (`KeyDataPointsBy`, `KeyDataPointsByParticipantDeviceTimeRange`)
or supply a pre-built `UtcOffsetMap` via `BuildMostCommonUtcOffsetMap`.

### Dataflow Label Escaping
The `regex` library was replaced with stdlib `re`. The Unicode-aware
pattern `\p{Ll}\p{Lo}\p{N}` is now ASCII `[a-z0-9_-]`. Since Dataflow
job labels are ASCII-only per the API spec, this is equivalent for all
valid inputs. Non-ASCII characters in labels were already invalid.

### Unpacking API
Use the specific PTransform classes (`UnpackImu`, `UnpackPpg`, `UnpackEcg`,
`UnpackEda`) instead of `DataUnpacker` directly. Example:
```python
from verily.raw_data_tools.unpacking import UnpackImu
unpacked = compressed_data | UnpackImu()
```

## Common Migration Issues

### Issue 1: Missing Registry Parameter
**Error**: `TypeError: __init__() missing 1 required positional argument: 'registry'`

**Solution**: Remove `registry` parameter, use `project` and `dataset` instead.

### Issue 2: Missing Study Info
**Error**: `AttributeError: 'RawDataIO' object has no attribute 'study_info'`

**Solution**: Study information is no longer needed. Access BigQuery tables directly.

### Issue 3: Import Errors
**Error**: `ModuleNotFoundError: No module named 'verily.ds_sdk'`

**Solution**: Update imports from `verily.ds_sdk.*` to `verily.raw_data_tools.*`

### Issue 4: Authentication Errors
**Error**: `google.auth.exceptions.DefaultCredentialsError`

**Solution**: 
- In Workbench: No action needed (automatic)
- Locally: Run `gcloud auth application-default login`

## Checklist for Migration

- [ ] Install `verily-raw-data-tools` (uninstall `verily-ds-sdk`)
- [ ] Update import statements
- [ ] Replace `SensorsIO` with `RawDataIO`
- [ ] Update initialization (remove `registry`, `env`, add `project`, `dataset`)
- [ ] Update `echo_data_point_rows()` to `read_datapoints()`
- [ ] Simplify conditions (use direct parameters instead of Condition objects)
- [ ] Update DataflowOptions (use `additional_options` dict for `temp_location`, `staging_location`, etc.)
- [ ] Test locally with DirectRunner
- [ ] Test on Dataflow with small dataset
- [ ] Deploy to production

## Getting Help

- **Documentation**: See `README.md`
- **Examples**: Check `examples/` directory
- **Issues**: https://github.com/vjeromeadamcote/verily-raw-data-tools/issues
- **Workbench Support**: workbench-support@verily.com

## Timeline

- **Now**: DS SDK (6.x) deprecated, Raw Data Tools (1.0) available
- **Q3 2026**: DS SDK support ends
- **Q4 2026**: DS SDK removed from distribution

Migrate as soon as possible to avoid disruption!
