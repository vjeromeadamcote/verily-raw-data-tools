# /sensor-explore

Explore sensor data availability in a Workbench Data Collection - show what data types exist, date ranges, and device coverage.

**Input:** $ARGUMENTS

## Step 1: Parse Arguments

Extract from `$ARGUMENTS`:

- **--collection <name>**: Data Collection name (default: auto-detect from workspace)
- **--detail**: Show detailed breakdown by device
- **--timeline**: Generate timeline visualization

Examples:
```
/sensor-explore
/sensor-explore --collection sensor_study_v2 --timeline
/sensor-explore --detail
```

## Step 2: Get Workspace Context

```python
import os
from google.cloud import bigquery
import pandas as pd

# Get workspace info
project = os.environ.get('GOOGLE_PROJECT')
workspace_id = os.environ.get('WORKSPACE_ID')

client = bigquery.Client(project=project)

print(f"📍 Workspace: {workspace_id}")
print(f"📍 Project: {project}\n")
```

## Step 3: Discover Data Collections

```python
# Find all datasets
query = f"""
SELECT 
  schema_name,
  (SELECT COUNT(*) 
   FROM `{project}.{schema_name}.INFORMATION_SCHEMA.TABLES`) as table_count
FROM `{project}.INFORMATION_SCHEMA.SCHEMATA`
WHERE schema_name NOT IN ('information_schema', 'INFORMATION_SCHEMA')
ORDER BY schema_name
"""

all_datasets = list(client.query(query))

# Filter for sensor-like datasets
sensor_datasets = [
    ds for ds in all_datasets 
    if 'sensor' in ds.schema_name.lower() 
    or 'datapoint' in ds.schema_name.lower()
    or 'study' in ds.schema_name.lower()
]

if collection_arg:
    # User specified a collection
    collection_name = collection_arg
    print(f"📚 Data Collection: {collection_name}")
    
elif len(sensor_datasets) == 1:
    collection_name = sensor_datasets[0].schema_name
    print(f"📚 Auto-detected Data Collection: {collection_name}")
    
elif len(sensor_datasets) > 1:
    print(f"📚 Found {len(sensor_datasets)} potential Data Collections:\n")
    for ds in sensor_datasets:
        print(f"   - {ds.schema_name} ({ds.table_count} tables)")
    
    print(f"\n💡 Specify one: /sensor-explore --collection {sensor_datasets[0].schema_name}")
    return
    
else:
    print("📚 All datasets in this workspace:\n")
    for ds in all_datasets:
        print(f"   - {ds.schema_name} ({ds.table_count} tables)")
    
    print("\n⚠️  No obvious sensor Data Collections found")
    print("💡 Specify one: /sensor-explore --collection <name>")
    return
```

## Step 4: Query Data Availability

### 4a. Overall summary by data type

```python
summary_query = f"""
SELECT 
  JSON_VALUE(DataPoint, '$.data_type') as data_type,
  COUNT(DISTINCT DeviceID) as unique_devices,
  COUNT(*) as total_datapoints,
  MIN(TIMESTAMP_MILLIS(CAST(DataPointTime AS INT64))) as earliest_datapoint,
  MAX(TIMESTAMP_MILLIS(CAST(DataPointTime AS INT64))) as latest_datapoint,
  TIMESTAMP_DIFF(
    MAX(TIMESTAMP_MILLIS(CAST(DataPointTime AS INT64))),
    MIN(TIMESTAMP_MILLIS(CAST(DataPointTime AS INT64))),
    DAY
  ) as days_span
FROM `{project}.{collection_name}.datapoint`
WHERE JSON_VALUE(DataPoint, '$.data_type') IS NOT NULL
GROUP BY data_type
ORDER BY total_datapoints DESC
"""

print(f"🔍 Analyzing data in {collection_name}...")
summary = list(client.query(summary_query))

if len(summary) == 0:
    print(f"❌ No data found in {collection_name}.datapoint")
    
    # Check what tables exist
    print(f"\nTables in {collection_name}:")
    tables = client.list_tables(f"{project}.{collection_name}")
    for table in tables:
        print(f"   - {table.table_id}")
    return

print(f"✅ Found {len(summary)} data types")
```

### 4b. Device coverage by type

```python
coverage_query = f"""
SELECT 
  JSON_VALUE(DataPoint, '$.data_type') as data_type,
  DeviceID as device_id,
  COUNT(*) as datapoint_count,
  MIN(TIMESTAMP_MILLIS(CAST(DataPointTime AS INT64))) as first_seen,
  MAX(TIMESTAMP_MILLIS(CAST(DataPointTime AS INT64))) as last_seen
FROM `{project}.{collection_name}.datapoint`
WHERE JSON_VALUE(DataPoint, '$.data_type') IS NOT NULL
GROUP BY data_type, device_id
ORDER BY data_type, datapoint_count DESC
"""

if show_detail:
    print("🔍 Loading device-level details...")
    coverage = list(client.query(coverage_query))
    coverage_df = pd.DataFrame([
        {
            'data_type': row.data_type,
            'device_id': row.device_id,
            'datapoint_count': row.datapoint_count,
            'first_seen': row.first_seen,
            'last_seen': row.last_seen
        }
        for row in coverage
    ])
```

### 4c. Sampling rate analysis (for unpacking)

```python
# Sample a few datapoints to show schema
sample_query = f"""
SELECT 
  JSON_VALUE(DataPoint, '$.data_type') as data_type,
  ARRAY_LENGTH(JSON_QUERY_ARRAY(DataPoint, '$.samples')) as sample_count,
  JSON_VALUE(DataPoint, '$.sampling_rate_hz') as sampling_rate
FROM `{project}.{collection_name}.datapoint`
WHERE JSON_VALUE(DataPoint, '$.data_type') IS NOT NULL
LIMIT 1000
"""

sample_data = list(client.query(sample_query))
sample_df = pd.DataFrame([
    {
        'data_type': row.data_type,
        'sample_count': row.sample_count,
        'sampling_rate': float(row.sampling_rate) if row.sampling_rate else None
    }
    for row in sample_data
])

# Calculate median samples per datapoint and sampling rates
sampling_stats = sample_df.groupby('data_type').agg({
    'sample_count': ['median', 'mean'],
    'sampling_rate': lambda x: x.dropna().mode()[0] if len(x.dropna()) > 0 else None
}).round(1)
```

## Step 5: Present Results

```markdown
## 🔬 Data Exploration: {collection_name}

**Workspace:** {workspace_id}  
**Data Collection:** {collection_name}  
**Analysis Date:** {datetime.now().date()}

### Overview

```python
# Summary table
summary_df = pd.DataFrame([
    {
        'Data Type': row.data_type,
        'Devices': row.unique_devices,
        'Datapoints': f"{row.total_datapoints:,}",
        'Earliest': row.earliest_datapoint.date(),
        'Latest': row.latest_datapoint.date(),
        'Span (days)': row.days_span
    }
    for row in summary
])

print(summary_df.to_markdown(index=False))

total_devices_all_types = sum(row.unique_devices for row in summary)
total_datapoints_all_types = sum(row.total_datapoints for row in summary)

print(f"\n**Totals:** {total_devices_all_types} unique devices, {total_datapoints_all_types:,} total datapoints")
```

### Sampling Information

This data collection uses **compressed datapoints** that need unpacking:

```python
print(sampling_stats.to_markdown())
print("\n*Use `/sensor-data` with `--unpack` to expand compressed datapoints into time series*")
```

{if show_detail}
### Device Coverage by Data Type

```python
# Show device counts per type
for data_type in summary_df['Data Type']:
    type_devices = coverage_df[coverage_df['data_type'] == data_type]
    device_count = len(type_devices)
    
    print(f"\n**{data_type}** ({device_count} devices):")
    
    # Top 10 devices by datapoint count
    top_devices = type_devices.nlargest(10, 'datapoint_count')
    
    for _, dev in top_devices.iterrows():
        days_span = (dev['last_seen'] - dev['first_seen']).days
        print(f"  - {dev['device_id']}: {dev['datapoint_count']:,} datapoints "
              f"({dev['first_seen'].date()} to {dev['last_seen'].date()}, {days_span} days)")
    
    if device_count > 10:
        print(f"  ... and {device_count - 10} more devices")
```
{endif}

### Data Timeline

```python
if show_timeline:
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 8))
    
    # Timeline of data availability by type
    for row in summary:
        ax1.plot([row.earliest_datapoint, row.latest_datapoint], 
                [row.data_type, row.data_type], 
                marker='o', linewidth=4, markersize=8, label=row.data_type)
    
    ax1.set_ylabel('Data Type', fontsize=10)
    ax1.set_xlabel('Date', fontsize=10)
    ax1.set_title('Data Availability Timeline by Type', fontsize=12, fontweight='bold')
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax1.grid(alpha=0.3, axis='x')
    plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    # Datapoint volume by type
    types = [row.data_type for row in summary]
    counts = [row.total_datapoints for row in summary]
    
    ax2.barh(types, counts, color='steelblue', alpha=0.7)
    ax2.set_xlabel('Total Datapoints', fontsize=10)
    ax2.set_ylabel('Data Type', fontsize=10)
    ax2.set_title('Datapoint Volume by Type', fontsize=12, fontweight='bold')
    ax2.set_xscale('log')
    ax2.grid(alpha=0.3, axis='x')
    
    # Add count labels
    for i, (t, c) in enumerate(zip(types, counts)):
        ax2.text(c, i, f' {c:,}', va='center', fontsize=9)
    
    plt.tight_layout()
    plt.show()
```

### Data Quality Insights

```python
from datetime import datetime, timedelta

now = datetime.now()

# Check data freshness
for row in summary:
    days_old = (now - row.latest_datapoint).days
    
    if days_old <= 7:
        status = "✅ Current (data from last 7 days)"
    elif days_old <= 30:
        status = "⚠️  Aging (last data {days_old} days ago)"
    else:
        status = f"❌ Stale (last data {days_old} days ago)"
    
    print(f"{row.data_type}: {status}")
```

### Next Steps

**Read data for a specific type:**
```
/sensor-data {summary[0].data_type} --since 7d --visualize
```

**List all devices:**
```
/sensor-devices --type {summary[0].data_type}
```

**Get specific device data:**
```python
# Pick a device from /sensor-devices
/sensor-data IMU,PPG --devices <DEVICE_ID> --since 30d --visualize --output results.parquet
```

**Unpack and analyze:**
```python
from verily.raw_data_tools import RawDataIO, DataUnpacker

io = RawDataIO(project='{project}', dataset='{collection_name}')
pipeline = io.create_pipeline()

# Read data
data = pipeline | io.read_datapoints(
    data_types=['{summary[0].data_type}'],
    start_time='2026-01-01',
    end_time='2026-01-31'
)

# Unpack
unpacker = DataUnpacker()
unpacked = data | unpacker

# Analyze...
```
```

## Summary Table Format

Creates a comprehensive overview table:

| Metric | Value |
|--------|-------|
| Total Data Types | {len(summary)} |
| Total Devices | {total_devices_all_types} |
| Total Datapoints | {total_datapoints_all_types:,} |
| Earliest Data | {min(row.earliest_datapoint for row in summary).date()} |
| Latest Data | {max(row.latest_datapoint for row in summary).date()} |
| Collection Span | {(max(row.latest_datapoint for row in summary) - min(row.earliest_datapoint for row in summary)).days} days |

## Notes

- Shows high-level data availability - use `/sensor-devices` for device-level details
- "Span" is the time between first and last datapoint for each data type
- Compressed datapoints need unpacking - each datapoint contains multiple samples
- Median samples/datapoint and sampling rate help estimate unpacked data volume
- Use `--timeline` for visual timeline of data availability
- Use `--detail` for per-device breakdown (may be slow on large collections)
