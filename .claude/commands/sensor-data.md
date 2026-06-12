# /sensor-data

Read, unpack, and analyze sensor data from Workbench BigQuery Data Collections. Uses `verily-raw-data-tools` library.

**Input:** $ARGUMENTS

## Step 1: Parse Arguments

Extract from `$ARGUMENTS`:

- **Device IDs** (optional): comma-separated device IDs (e.g., `ABC123,XYZ789`)
- **Data types** (required): comma-separated sensor types (e.g., `IMU,PPG,ECG`)
- **Date range** (required): `--since <duration>` or `--start <date> --end <date>`
  - Duration: `1h`, `6h`, `1d`, `7d`, `30d`
  - Dates: ISO format `2026-01-01`
- **--devices <ids>**: Comma-separated device IDs
- **--collection <name>**: Data Collection name (default: auto-detect from workspace)
- **--env <environment>**: `prod`, `dev-stable` (default: current workspace env)
- **--visualize**: Generate matplotlib visualizations
- **--unpack**: Unpack compressed sensor data (default: true)
- **--output <path>**: Save results to file (CSV or Parquet)

Examples:
```
/sensor-data IMU,PPG --since 7d --visualize
/sensor-data IMU --devices ABC123,XYZ456 --start 2026-01-01 --end 2026-01-31
/sensor-data PPG --collection sensor_study_v2 --visualize --output results.parquet
```

## Step 2: Discover Workspace Context

### 2a. Get current workspace info

```python
import os
import subprocess
import json

# Workbench provides these via environment
project = os.environ.get('GOOGLE_PROJECT')
workspace_id = os.environ.get('WORKSPACE_ID')

# If not in environment, use Workbench CLI
if not project:
    result = subprocess.run(
        ['wb', 'workspace', 'describe', '--format=json'],
        capture_output=True,
        text=True
    )
    if result.returncode == 0:
        workspace_info = json.loads(result.stdout)
        project = workspace_info.get('googleProject')
        workspace_id = workspace_info.get('id')

print(f"📍 Workspace: {workspace_id}")
print(f"📍 GCP Project: {project}")
```

### 2b. List available Data Collections

If `--collection` not specified, discover available datasets:

```python
from google.cloud import bigquery

client = bigquery.Client(project=project)

# Query for datasets that look like sensor data
query = f"""
SELECT 
  schema_name as dataset_id,
  COUNT(*) as table_count
FROM `{project}.INFORMATION_SCHEMA.SCHEMATA`
WHERE schema_name LIKE '%sensor%' 
   OR schema_name LIKE '%datapoint%'
   OR schema_name LIKE '%study%'
GROUP BY schema_name
ORDER BY schema_name
"""

datasets = list(client.query(query))

if len(datasets) == 0:
    print("⚠️  No sensor data collections found in this workspace.")
    print("\n💡 Available datasets:")
    
    # Show all datasets
    all_datasets = list(client.list_datasets())
    for ds in all_datasets:
        print(f"   - {ds.dataset_id}")
    
    print("\nSpecify a dataset with --collection <name>")
    return

elif len(datasets) == 1:
    # Auto-select the only collection
    collection_name = datasets[0].dataset_id
    print(f"✅ Auto-detected Data Collection: {collection_name}")

else:
    # Multiple collections - ask user
    print("📚 Available Data Collections in this workspace:\n")
    for i, ds in enumerate(datasets, 1):
        print(f"{i}. {ds.dataset_id} ({ds.table_count} tables)")
    
    print("\n💡 Specify which collection with --collection <name>")
    print(f"   Example: /sensor-data IMU --collection {datasets[0].dataset_id}")
    return
```

### 2c. Verify table schema

```python
# Check if the collection has a 'datapoint' table
table_ref = f"{project}.{collection_name}.datapoint"

try:
    table = client.get_table(table_ref)
    
    # Verify expected sensor data columns
    schema_cols = [field.name for field in table.schema]
    required_cols = ['DeviceID', 'DataPointTime', 'DataPoint']
    
    missing_cols = [col for col in required_cols if col not in schema_cols]
    
    if missing_cols:
        print(f"⚠️  Table {table_ref} missing expected columns: {missing_cols}")
        print(f"\nAvailable columns: {schema_cols}")
        return
    
    print(f"✅ Data Collection verified: {collection_name}")
    print(f"   Table: {table.num_rows:,} rows")
    
except Exception as e:
    print(f"❌ Error accessing table {table_ref}: {e}")
    
    # List tables in the dataset
    print(f"\nAvailable tables in {collection_name}:")
    tables = client.list_tables(f"{project}.{collection_name}")
    for tbl in tables:
        print(f"   - {tbl.table_id}")
    return
```

## Step 3: Read Sensor Data

### 3a. Use verily-raw-data-tools

```python
from verily.raw_data_tools import RawDataIO
from datetime import datetime, timedelta
import pandas as pd

# Parse time range
def parse_duration(duration_str):
    """Parse duration like '7d', '1h', '30d' into timedelta"""
    unit = duration_str[-1]
    value = int(duration_str[:-1])
    
    if unit == 'h':
        return timedelta(hours=value)
    elif unit == 'd':
        return timedelta(days=value)
    else:
        raise ValueError(f"Unknown duration unit: {unit}")

if since_duration:
    end_time = datetime.now()
    start_time = end_time - parse_duration(since_duration)
else:
    start_time = datetime.fromisoformat(start_date)
    end_time = datetime.fromisoformat(end_date)

# Initialize with Workbench context
io = RawDataIO(
    project=project,
    dataset=collection_name,
    runner='DirectRunner'  # Local execution in Workbench
)

# Read data
print(f"\n📊 Reading sensor data...")
print(f"   Data types: {data_types}")
print(f"   Time range: {start_time.date()} to {end_time.date()}")
if device_ids:
    print(f"   Devices: {device_ids}")

pipeline = io.create_pipeline()

data = pipeline | io.read_datapoints(
    device_ids=device_ids.split(',') if device_ids else None,
    data_types=data_types.split(','),
    start_time=start_time.isoformat(),
    end_time=end_time.isoformat()
)

# Run pipeline and collect results
result = pipeline.run()
result.wait_until_finish()

print(f"✅ Query complete")
```

### 3b. Handle empty results

```python
if len(data) == 0:
    print("\n⚠️  No data found. Possible reasons:")
    print("   - Device IDs don't exist in this collection")
    print("   - Date range has no data")
    print("   - Data types not available")
    
    print("\n💡 Try these commands to explore:")
    print("   /sensor-explore    - Show data availability")
    print("   /sensor-devices    - List available devices")
    return
```

## Step 4: Unpack Data (if requested)

```python
if should_unpack:
    from verily.raw_data_tools import DataUnpacker
    import apache_beam as beam
    
    print("\n🔓 Unpacking sensor data...")
    
    unpacker = DataUnpacker(
        error_thresh=0.05,  # 5% sampling rate tolerance
        ignore_median_fs_error=False
    )
    
    # Apply unpacking transform
    unpacked = data | 'Unpack' >> beam.ParDo(unpacker)
    
    # Convert to DataFrame
    from verily.raw_data_tools.transforms import BuildDataFrames
    
    dataframes = unpacked | 'Build DataFrames' >> BuildDataFrames(
        include_metadata=True,
        sort_by_time=True
    )
    
    # Collect results into single DataFrame
    # (In practice, this would be a PCollection - adapt based on your needs)
    unpacked_df = pd.concat([df for key, df in dataframes], ignore_index=True)
    
    print(f"✅ Unpacked to {len(unpacked_df):,} rows")
    print(f"\nData shape: {unpacked_df.shape}")
    print(f"Time range: {unpacked_df['timestamp'].min()} to {unpacked_df['timestamp'].max()}")
    print(f"Unique devices: {unpacked_df['device_id'].nunique()}")
    
else:
    # Use raw data without unpacking
    unpacked_df = data
```

## Step 5: Visualize (if requested)

```python
import matplotlib.pyplot as plt
import seaborn as sns

if should_visualize:
    print("\n📈 Creating visualizations...")
    
    data_type_list = data_types.split(',')
    
    # IMU Data Visualization
    if 'IMU' in data_type_list:
        fig, axes = plt.subplots(3, 1, figsize=(14, 10), sharex=True)
        
        if 'accel_x' in unpacked_df.columns:
            axes[0].plot(unpacked_df['timestamp'], unpacked_df['accel_x'], 
                        alpha=0.7, linewidth=0.5)
            axes[0].set_ylabel('Accel X (g)', fontsize=10)
            axes[0].grid(alpha=0.3)
            axes[0].set_title('IMU Acceleration Data', fontsize=12, fontweight='bold')
            
            axes[1].plot(unpacked_df['timestamp'], unpacked_df['accel_y'], 
                        alpha=0.7, linewidth=0.5, color='orange')
            axes[1].set_ylabel('Accel Y (g)', fontsize=10)
            axes[1].grid(alpha=0.3)
            
            axes[2].plot(unpacked_df['timestamp'], unpacked_df['accel_z'], 
                        alpha=0.7, linewidth=0.5, color='green')
            axes[2].set_ylabel('Accel Z (g)', fontsize=10)
            axes[2].set_xlabel('Time', fontsize=10)
            axes[2].grid(alpha=0.3)
            
            plt.tight_layout()
            plt.show()
        
        # Magnitude plot
        if 'accel_magnitude' in unpacked_df.columns:
            plt.figure(figsize=(14, 5))
            plt.plot(unpacked_df['timestamp'], unpacked_df['accel_magnitude'], 
                    linewidth=0.8)
            plt.xlabel('Time', fontsize=10)
            plt.ylabel('Acceleration Magnitude (g)', fontsize=10)
            plt.title('Overall Acceleration Magnitude', fontsize=12, fontweight='bold')
            plt.grid(alpha=0.3)
            plt.tight_layout()
            plt.show()
    
    # PPG Data Visualization
    if 'PPG' in data_type_list:
        plt.figure(figsize=(14, 5))
        
        if 'ppg_value' in unpacked_df.columns:
            plt.plot(unpacked_df['timestamp'], unpacked_df['ppg_value'], 
                    alpha=0.7, linewidth=0.6, color='red')
            plt.xlabel('Time', fontsize=10)
            plt.ylabel('PPG Value', fontsize=10)
            plt.title('PPG Signal', fontsize=12, fontweight='bold')
            plt.grid(alpha=0.3)
            plt.tight_layout()
            plt.show()
    
    # Multi-device comparison (if multiple devices)
    if unpacked_df['device_id'].nunique() > 1:
        plt.figure(figsize=(14, 6))
        
        # Determine value column based on data type
        value_col = 'accel_magnitude' if 'accel_magnitude' in unpacked_df.columns else 'value'
        
        sns.lineplot(
            data=unpacked_df,
            x='timestamp',
            y=value_col,
            hue='device_id',
            alpha=0.7,
            linewidth=1.5
        )
        plt.title('Multi-Device Comparison', fontsize=12, fontweight='bold')
        plt.xlabel('Time', fontsize=10)
        plt.ylabel('Value', fontsize=10)
        plt.legend(title='Device', bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.grid(alpha=0.3)
        plt.tight_layout()
        plt.show()
    
    # Distribution analysis
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
    
    value_col = 'accel_magnitude' if 'accel_magnitude' in unpacked_df.columns else 'value'
    
    # Histogram
    ax1.hist(unpacked_df[value_col].dropna(), bins=50, edgecolor='black', alpha=0.7)
    ax1.set_xlabel('Value', fontsize=10)
    ax1.set_ylabel('Frequency', fontsize=10)
    ax1.set_title('Value Distribution', fontsize=11, fontweight='bold')
    ax1.grid(alpha=0.3, axis='y')
    
    # Box plot by device
    if unpacked_df['device_id'].nunique() > 1 and unpacked_df['device_id'].nunique() <= 10:
        sns.boxplot(data=unpacked_df, x='device_id', y=value_col, ax=ax2)
        ax2.set_title('Distribution by Device', fontsize=11, fontweight='bold')
        ax2.set_xlabel('Device ID', fontsize=10)
        ax2.set_ylabel('Value', fontsize=10)
        ax2.tick_params(axis='x', rotation=45)
    else:
        # Too many devices - show overall stats
        ax2.text(0.5, 0.5, f'{unpacked_df["device_id"].nunique()} devices\n(too many for box plot)', 
                ha='center', va='center', fontsize=12, transform=ax2.transAxes)
        ax2.set_title('Device Count', fontsize=11, fontweight='bold')
        ax2.axis('off')
    
    plt.tight_layout()
    plt.show()
    
    print("✅ Visualizations complete")
```

## Step 6: Save Output (if requested)

```python
if output_path:
    print(f"\n💾 Saving results to {output_path}...")
    
    if output_path.endswith('.csv'):
        unpacked_df.to_csv(output_path, index=False)
    elif output_path.endswith('.parquet'):
        unpacked_df.to_parquet(output_path, index=False)
    else:
        print("⚠️  Unknown file format. Supported: .csv, .parquet")
        print("   Saving as CSV...")
        unpacked_df.to_csv(output_path + '.csv', index=False)
        output_path = output_path + '.csv'
    
    print(f"✅ Saved {len(unpacked_df):,} rows to {output_path}")
```

## Step 7: Present Results

```markdown
## 📊 Sensor Data Analysis Results

**Workspace:** {workspace_id}  
**Data Collection:** {collection_name}  
**Data Types:** {data_types}  
**Time Range:** {start_time.date()} to {end_time.date()}  
**Devices:** {device_ids if device_ids else "all devices"}

### Summary Statistics

- **Total Datapoints:** {original_row_count:,}
- **Unpacked Rows:** {len(unpacked_df):,}
- **Unique Devices:** {unpacked_df['device_id'].nunique()}
- **Actual Date Range:** {unpacked_df['timestamp'].min()} to {unpacked_df['timestamp'].max()}
- **Duration:** {(unpacked_df['timestamp'].max() - unpacked_df['timestamp'].min()).total_seconds() / 3600:.1f} hours

### Data Preview

```python
# The data is available as: unpacked_df
print(unpacked_df.head(10))
print(f"\nShape: {unpacked_df.shape}")
print(f"Columns: {unpacked_df.columns.tolist()}")

# Quick stats
print("\nBasic Statistics:")
print(unpacked_df.describe())
```

### Next Steps

You can continue analyzing:

```python
# Filter by specific device
device_df = unpacked_df[unpacked_df['device_id'] == 'ABC123']

# Time-based filtering
import pandas as pd
recent_data = unpacked_df[unpacked_df['timestamp'] > pd.Timestamp('2026-01-15')]

# Apply custom analysis with verily-raw-data-tools
from verily.raw_data_tools.transforms import apply_to_dataframe

def custom_analysis(df):
    # Your analysis code here
    df['rolling_mean'] = df['value'].rolling(window=100).mean()
    return df

analyzed = apply_to_dataframe(custom_analysis)(unpacked_df)

# Export for further analysis
unpacked_df.to_csv('my_sensor_data.csv', index=False)
unpacked_df.to_parquet('my_sensor_data.parquet')  # More efficient
```

### Available Commands

- `/sensor-devices` - List all devices in this Data Collection
- `/sensor-explore` - Explore data availability by type and date
- `/sensor-data --help` - Show all options
```

## Notes

- Uses `verily-raw-data-tools` library - install with: `pip install verily-raw-data-tools`
- Workbench automatically handles BigQuery authentication
- All data stays within the secure Workbench environment
- Visualizations use matplotlib (not interactive dashboards) - keeps data scientists in control
- For large datasets, consider adding `--limit` to the BigQuery query
