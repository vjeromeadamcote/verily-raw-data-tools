# /sensor-devices

List all devices in a Workbench Data Collection with sensor data availability and statistics.

**Input:** $ARGUMENTS

## Step 1: Parse Arguments

Extract from `$ARGUMENTS`:

- **--collection <name>**: Data Collection name (default: auto-detect from workspace)
- **--type <data_type>**: Filter by data type (e.g., `IMU`, `PPG`, `ECG`)
- **--since <duration>**: Show only devices active in time window (e.g., `7d`, `30d`)
- **--limit <n>**: Show top N devices by datapoint count (default: 50)
- **--sort**: Sort order - `count` (default), `recent`, `name`

Examples:
```
/sensor-devices
/sensor-devices --collection sensor_study_v2
/sensor-devices --type IMU --since 30d
/sensor-devices --sort recent --limit 20
```

## Step 2: Get Workspace Context

```python
import os
from google.cloud import bigquery

# Get workspace info
project = os.environ.get('GOOGLE_PROJECT')
workspace_id = os.environ.get('WORKSPACE_ID')

client = bigquery.Client(project=project)

print(f"📍 Workspace: {workspace_id}")
print(f"📍 Project: {project}")
```

## Step 3: Determine Data Collection

### If --collection specified

```python
collection_name = collection_arg
print(f"📚 Data Collection: {collection_name}")
```

### If not specified, auto-detect

```python
# Find datasets with sensor data
query = f"""
SELECT schema_name
FROM `{project}.INFORMATION_SCHEMA.SCHEMATA`
WHERE schema_name LIKE '%sensor%'
   OR schema_name LIKE '%datapoint%'
   OR schema_name LIKE '%study%'
ORDER BY schema_name
"""

collections = [row.schema_name for row in client.query(query)]

if len(collections) == 0:
    print("❌ No Data Collections found")
    print("\n💡 Specify with: /sensor-devices --collection <name>")
    return
elif len(collections) == 1:
    collection_name = collections[0]
    print(f"📚 Auto-detected: {collection_name}")
else:
    print(f"📚 Found {len(collections)} Data Collections:")
    for coll in collections:
        print(f"   - {coll}")
    print(f"\n💡 Specify which one: /sensor-devices --collection {collections[0]}")
    return
```

## Step 4: Query Device List

### 4a. Build query based on filters

```python
# Base query
base_query = f"""
SELECT 
  DeviceID as device_id,
  JSON_VALUE(DataPoint, '$.data_type') as data_type,
  COUNT(*) as datapoint_count,
  MIN(TIMESTAMP_MILLIS(CAST(DataPointTime AS INT64))) as first_seen,
  MAX(TIMESTAMP_MILLIS(CAST(DataPointTime AS INT64))) as last_seen,
  TIMESTAMP_DIFF(
    MAX(TIMESTAMP_MILLIS(CAST(DataPointTime AS INT64))),
    MIN(TIMESTAMP_MILLIS(CAST(DataPointTime AS INT64))),
    DAY
  ) as days_active
FROM `{project}.{collection_name}.datapoint`
WHERE 1=1
"""

# Add type filter
if data_type_filter:
    base_query += f"  AND JSON_VALUE(DataPoint, '$.data_type') = '{data_type_filter}'\n"

# Add time filter
if since_duration:
    base_query += f"""  AND TIMESTAMP_MILLIS(CAST(DataPointTime AS INT64)) > 
    TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {since_value} {since_unit})\n"""

# Group and sort
base_query += """
GROUP BY device_id, data_type
"""

if sort_order == 'count':
    base_query += "ORDER BY datapoint_count DESC\n"
elif sort_order == 'recent':
    base_query += "ORDER BY last_seen DESC\n"
elif sort_order == 'name':
    base_query += "ORDER BY device_id ASC\n"

# Add limit
base_query += f"LIMIT {limit_value}"

print(f"\n🔍 Querying devices in {collection_name}...")
```

### 4b. Execute query

```python
devices = list(client.query(base_query))

if len(devices) == 0:
    print("❌ No devices found with the specified filters")
    print("\n💡 Try:")
    print("   - Remove filters: /sensor-devices")
    print("   - Check data type: /sensor-explore")
    return

print(f"✅ Found {len(devices)} devices")
```

## Step 5: Calculate Statistics

```python
import pandas as pd
from datetime import datetime

# Convert to DataFrame for easier analysis
devices_df = pd.DataFrame([
    {
        'device_id': row.device_id,
        'data_type': row.data_type,
        'datapoint_count': row.datapoint_count,
        'first_seen': row.first_seen,
        'last_seen': row.last_seen,
        'days_active': row.days_active
    }
    for row in devices
])

# Summary statistics
total_devices = devices_df['device_id'].nunique()
total_datapoints = devices_df['datapoint_count'].sum()
data_types = devices_df['data_type'].unique()
earliest_data = devices_df['first_seen'].min()
latest_data = devices_df['last_seen'].max()

# Calculate device activity status
now = datetime.now()
devices_df['days_since_last'] = devices_df['last_seen'].apply(
    lambda x: (now - x).days if pd.notna(x) else None
)

active_devices = len(devices_df[devices_df['days_since_last'] <= 7])
inactive_devices = len(devices_df[devices_df['days_since_last'] > 30])
```

## Step 6: Present Results

```markdown
## 📱 Device List: {collection_name}

**Workspace:** {workspace_id}  
**Data Collection:** {collection_name}  
**Filters:** {f"Type: {data_type_filter}" if data_type_filter else "All types"} | {f"Since: {since_duration}" if since_duration else "All time"}

### Summary

- **Total Devices:** {total_devices}
- **Total Datapoints:** {total_datapoints:,}
- **Data Types:** {', '.join(data_types)}
- **Date Range:** {earliest_data.date()} to {latest_data.date()}
- **Active (last 7 days):** {active_devices}
- **Inactive (>30 days):** {inactive_devices}

### Device List

| Device ID | Data Type | Datapoints | First Seen | Last Seen | Days Active |
|-----------|-----------|------------|------------|-----------|-------------|
```

```python
# Print table rows
for _, row in devices_df.head(limit_value).iterrows():
    days_since = f"{row['days_since_last']:.0f}d ago" if pd.notna(row['days_since_last']) else "N/A"
    
    print(f"| {row['device_id']} "
          f"| {row['data_type']} "
          f"| {row['datapoint_count']:,} "
          f"| {row['first_seen'].date()} "
          f"| {row['last_seen'].date()} ({days_since}) "
          f"| {row['days_active']} |")
```

```markdown
{f"*Showing top {limit_value} of {total_devices} devices*" if total_devices > limit_value else ""}

### Device Activity Distribution

```python
import matplotlib.pyplot as plt
import seaborn as sns

# Plot 1: Datapoints per device
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

# Histogram of datapoint counts
ax1.hist(devices_df['datapoint_count'], bins=30, edgecolor='black', alpha=0.7)
ax1.set_xlabel('Datapoints per Device', fontsize=10)
ax1.set_ylabel('Number of Devices', fontsize=10)
ax1.set_title('Datapoint Distribution', fontsize=11, fontweight='bold')
ax1.set_yscale('log')
ax1.grid(alpha=0.3, axis='y')

# Timeline of device activity
ax2.scatter(devices_df['first_seen'], devices_df['device_id'], 
           alpha=0.5, s=20, label='First Seen', color='green')
ax2.scatter(devices_df['last_seen'], devices_df['device_id'], 
           alpha=0.5, s=20, label='Last Seen', color='red')
ax2.set_xlabel('Date', fontsize=10)
ax2.set_ylabel('Device ID', fontsize=10)
ax2.set_title('Device Activity Timeline', fontsize=11, fontweight='bold')
ax2.legend()
ax2.tick_params(axis='y', labelleft=False)  # Hide device IDs if too many
ax2.grid(alpha=0.3, axis='x')

plt.tight_layout()
plt.show()
```

### Next Steps

**Get data for a specific device:**
```python
/sensor-data IMU,PPG --devices ABC123 --since 7d --visualize
```

**Explore device in detail:**
```python
# Use the device ID from the table above
device_id = 'ABC123'

# Read all data for this device
from verily.raw_data_tools import RawDataIO

io = RawDataIO(project='{project}', dataset='{collection_name}')
pipeline = io.create_pipeline()

device_data = pipeline | io.read_datapoints(
    device_ids=[device_id],
    data_types=['IMU', 'PPG']
)
```

**Filter by activity:**
```python
# Active devices only
active_devices = devices_df[devices_df['days_since_last'] <= 7]
print(active_devices['device_id'].tolist())

# Get data for all active devices
/sensor-data IMU --devices {','.join(active_devices['device_id'].tolist()[:5])} --since 7d
```
```

## Options Reference

| Flag | Description | Example |
|------|-------------|---------|
| `--collection <name>` | Specify Data Collection | `--collection sensor_study_v2` |
| `--type <type>` | Filter by data type | `--type IMU` |
| `--since <duration>` | Only show devices active in window | `--since 30d` |
| `--limit <n>` | Limit results | `--limit 100` |
| `--sort <order>` | Sort by count/recent/name | `--sort recent` |

## Notes

- Device list is based on BigQuery `datapoint` table in the Data Collection
- "Days since last" calculated from current time to last datapoint timestamp
- Active = data in last 7 days, Inactive = no data in 30+ days
- Large collections may take longer to query - use `--limit` to speed up
- Use `/sensor-explore` to see data types and availability across all devices
