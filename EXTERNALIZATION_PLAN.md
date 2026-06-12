# Verily Raw Data Tools - Externalization Plan

## Overview
Creating an externalized version of the DS SDK called "Verily Raw Data Tools" for Workbench users.

**Version**: 1.0.0  
**Package Name**: `verily-raw-data-tools`  
**Target Users**: Workbench customers with BQ access

## Core Capabilities (What to Keep)

### 1. Data Read
**Modules to Keep**:
- `verily/ds_sdk/core/sensors_io.py` - Main I/O interface
- `verily/ds_sdk/core/data_filters.py` - Filtering and deduplication
- `verily/ds_sdk/core/io/bigquery/data_points_source.py` - BQ data reading
- `verily/ds_sdk/core/conditions.py` - Query conditions
- `verily/ds_sdk/core/schemas.py` - Data schemas

**What to Remove/Make Optional**:
- `verily/ds_sdk/core/sensorsuite/sensor_store_client.py` - Internal SensorStore (REMOVE)
- `verily/ds_sdk/core/grpc/registry_service.py` - Internal gRPC calls (REMOVE)
- `verily/ds_sdk/core/io/pubsub/` - Streaming from PubSub (REMOVE for v1.0)
- Internal auth/credentials - Use Workbench's BQ access instead

### 2. Unpacking
**Modules to Keep**:
- `verily/ds_sdk/contrib/data_unpacking.py` ✅
- `verily/ds_sdk/contrib/data_unpacking_numba.py` ✅
- `verily/ds_sdk/contrib/data_unpacking_legacy.py` ✅

### 3. Transform DataPoint → DataFrames
**Modules to Keep**:
- `verily/ds_sdk/core/transforms/composite/build_data_frames.py` ✅
- `verily/ds_sdk/core/transforms/atomic/group_into_data_frames.py` ✅

### 4. Data Segmentation and Key-By
**Modules to Keep**:
- `verily/ds_sdk/core/transforms/atomic/key_by.py` ✅
- Time windowing utilities

### 5. 3P Algorithm Integration
**What to Add**:
- Entry point for user-defined Python functions
- Beam transform wrapper for custom algorithms
- Examples showing how to plug in external algorithms

### 6. Pipeline Building and Launching
**Modules to Keep**:
- `verily/ds_sdk/core/docker/worker_image.py` - Docker building ✅
- `verily/ds_sdk/core/utils/dataflow_utils.py` - Dataflow utilities ✅
- `verily/ds_sdk/core/options.py` - Pipeline options ✅
- `verily/ds_sdk/core/utils/runner_utils.py` - Runner helpers ✅

## Dependencies to Handle

### Internal Dependencies (REMOVE)
1. **SensorStore Client** (`verily.ds_sdk.core.sensorsuite.sensor_store_client`)
   - Used for reading from internal Verily sensor storage
   - **Action**: Remove completely, users will read from BQ only

2. **gRPC Registry Service** (`verily.ds_sdk.core.grpc.registry_service`)
   - Used for accessing internal registry metadata
   - **Action**: Remove, replace with simplified config

3. **Internal Credentials** (`verily.ds_sdk.core.gcp.credentials`)
   - Verily-specific auth
   - **Action**: Replace with standard Google Cloud auth or Workbench's auth

4. **StudyInfo/Studies** (`verily.ds_sdk.core.studies`)
   - Internal study management
   - **Action**: Make optional or simplify to basic project/dataset config

### External Dependencies (KEEP but simplify)
- `apache-beam[gcp]` ✅
- `google-cloud-bigquery` ✅
- `google-cloud-storage` ✅
- `pandas` ✅
- `numpy` ✅
- `numba` (for unpacking) ✅

### Dependencies to Remove
- `google-cloud-secret-manager` - Internal use
- `redis` - For streaming cache (not needed for v1.0)
- `google-cloud-build` - May not be needed if Docker build is simplified

## Package Structure

```
verily-raw-data-tools/
├── verily/
│   └── raw_data_tools/
│       ├── __init__.py
│       ├── io/
│       │   ├── __init__.py
│       │   ├── bigquery_source.py      # Simplified BQ reading
│       │   └── data_filters.py         # Filtering/deduplication
│       ├── unpacking/
│       │   ├── __init__.py
│       │   ├── data_unpacking.py
│       │   └── data_unpacking_numba.py
│       ├── transforms/
│       │   ├── __init__.py
│       │   ├── build_data_frames.py
│       │   ├── key_by.py
│       │   └── custom_transform.py     # NEW: 3P algo integration
│       ├── pipeline/
│       │   ├── __init__.py
│       │   ├── options.py
│       │   ├── docker_builder.py
│       │   └── dataflow_utils.py
│       ├── schemas.py
│       └── conditions.py
├── pyproject.toml
├── README.md
└── examples/
    ├── basic_data_read.py
    ├── unpack_and_transform.py
    ├── custom_algorithm.py
    └── full_pipeline.py
```

## Implementation Steps

1. **Create new package structure** (keep original SDK intact for now)
2. **Copy and refactor core modules**:
   - Remove internal dependencies
   - Simplify authentication to use Workbench/standard GCP auth
   - Update imports to new package name
3. **Create simplified entry point** for Workbench users
4. **Write examples and documentation**
5. **Update pyproject.toml**:
   - Change name to `verily-raw-data-tools`
   - Remove "Private :: Do Not Upload" classifier
   - Set version to 1.0.0
   - Clean up dependencies
6. **Test with Workbench environment**

## Key Simplifications for External Users

1. **No Registry Concept**: Users provide GCP project and BQ dataset directly
2. **No Internal Auth**: Use standard GCP auth (ADC or service accounts)
3. **Simplified Configuration**: Direct parameters instead of environment-based lookups
4. **Clear Examples**: Show common workflows (read → unpack → transform → analyze)

## Migration Path

For internal Verily users who want to migrate:
- Keep `verily-ds-sdk` for internal use
- Use `verily-raw-data-tools` for Workbench/external deployment
- Can use both in same environment if needed (different import paths)

## Next Steps

1. Implement the new package structure
2. Update pyproject.toml
3. Create documentation and examples
4. Test in Workbench environment
5. Create PR for review
