# Verily Raw Data Tools - Claude Skills

Claude Code commands for working with sensor data in Verily Workbench.

## Commands

| Command | Description |
|---------|-------------|
| `/sensor-data` | Read, unpack, and analyze sensor data from Workbench BigQuery Data Collections |
| `/sensor-devices` | List all devices in a Data Collection with availability statistics |
| `/sensor-explore` | Explore data availability - what types exist, date ranges, device coverage |

## Quick Start

### Explore what data is available

```
/sensor-explore
```

Shows all data types, date ranges, and device counts in your workspace's Data Collections.

### List devices

```
/sensor-devices --type IMU
```

See all devices that have IMU data, when they were active, and how many datapoints each has.

### Read and visualize data

```
/sensor-data IMU,PPG --since 7d --visualize
```

Reads IMU and PPG data from the last 7 days, unpacks it, and creates matplotlib visualizations.

## Detailed Command Reference

### /sensor-data

**Purpose:** Main command for reading, unpacking, and analyzing sensor data.

**Usage:**
```
/sensor-data <data_types> [options]
```

**Arguments:**
- `<data_types>` (required): Comma-separated sensor types (e.g., `IMU,PPG,ECG`)
- `--devices <ids>`: Filter by device IDs (comma-separated)
- `--since <duration>`: Time window (e.g., `1h`, `7d`, `30d`)
- `--start <date> --end <date>`: Explicit date range (ISO format: `2026-01-01`)
- `--collection <name>`: Specify Data Collection (auto-detected if not provided)
- `--visualize`: Generate matplotlib plots
- `--output <path>`: Save results to CSV or Parquet file
- `--unpack`: Unpack compressed sensor data (default: true)

**Examples:**
```
# Get IMU data from last week with visualizations
/sensor-data IMU --since 7d --visualize

# Get specific devices' data
/sensor-data IMU,PPG --devices ABC123,XYZ789 --since 30d

# Read and save to file
/sensor-data PPG --start 2026-01-01 --end 2026-01-31 --output results.parquet

# From specific Data Collection
/sensor-data ECG --collection clinical_sensors --since 1d
```

**What it does:**
1. Auto-detects your workspace's Data Collections
2. Queries BigQuery for sensor data
3. Uses `verily-raw-data-tools` to unpack compressed datapoints
4. Creates visualizations (matplotlib time series, distributions, multi-device comparisons)
5. Returns a pandas DataFrame you can analyze further

### /sensor-devices

**Purpose:** List all devices in a Data Collection with statistics.

**Usage:**
```
/sensor-devices [options]
```

**Arguments:**
- `--collection <name>`: Specify Data Collection
- `--type <data_type>`: Filter by sensor type (e.g., `IMU`)
- `--since <duration>`: Show only devices active in time window
- `--limit <n>`: Show top N devices (default: 50)
- `--sort <order>`: Sort by `count` (default), `recent`, or `name`

**Examples:**
```
# List all devices
/sensor-devices

# IMU devices active in last month
/sensor-devices --type IMU --since 30d

# Top 20 most active devices
/sensor-devices --sort count --limit 20

# Recent devices first
/sensor-devices --sort recent
```

**What it does:**
1. Queries BigQuery for unique devices
2. Shows datapoint counts, first/last seen dates, activity status
3. Displays distribution charts (datapoints per device, activity timeline)
4. Helps you identify which devices to analyze

### /sensor-explore

**Purpose:** Get a high-level overview of what data exists in a Data Collection.

**Usage:**
```
/sensor-explore [options]
```

**Arguments:**
- `--collection <name>`: Specify Data Collection
- `--detail`: Show per-device breakdown
- `--timeline`: Generate timeline visualization

**Examples:**
```
# Quick overview
/sensor-explore

# Detailed view with timeline
/sensor-explore --timeline --detail

# Specific collection
/sensor-explore --collection sensor_study_v2
```

**What it does:**
1. Lists all available Data Collections in workspace
2. Shows data types, device counts, date ranges
3. Displays sampling rate information
4. Shows data quality insights (freshness, coverage)
5. Creates timeline visualizations of data availability

## How It Works

### Workbench Integration

These commands are designed specifically for Verily Workbench:

- **Auto-authentication**: Uses `GOOGLE_PROJECT` environment variable
- **Data Collections**: Discovers attached BigQuery datasets automatically
- **Workspace-aware**: Queries only data accessible in current workspace
- **Secure**: All data stays within Workbench environment

### Uses verily-raw-data-tools

The commands use the `verily-raw-data-tools` Python library:

```python
from verily.raw_data_tools import RawDataIO, DataUnpacker
from verily.raw_data_tools.transforms import BuildDataFrames, KeyBy
```

Make sure the library is installed in your Workbench environment:
```bash
pip install verily-raw-data-tools
```

### Output Format

Commands return:
- **Text summaries** with key statistics
- **Pandas DataFrames** for further analysis
- **Matplotlib visualizations** (not interactive dashboards - keeps you in control)
- **Working Python code** you can copy, modify, and reuse

## Workflow Examples

### Explore → Devices → Analyze

```
# 1. What data do I have?
/sensor-explore

# 2. Which devices have IMU data?
/sensor-devices --type IMU --since 30d

# 3. Get data for top device
/sensor-data IMU --devices <DEVICE_ID> --since 30d --visualize
```

### Quick Analysis

```
# Get last week's data with plots
/sensor-data IMU,PPG --since 7d --visualize

# Results are in `unpacked_df` variable
# Continue analyzing in Python:
unpacked_df.describe()
unpacked_df.groupby('device_id')['value'].mean()
```

### Export for Offline Analysis

```
# Save to Parquet (more efficient)
/sensor-data IMU,PPG --since 30d --output sensor_data.parquet

# Later, load and analyze
import pandas as pd
df = pd.read_parquet('sensor_data.parquet')
```

## Visualization Examples

The `--visualize` flag creates publication-quality matplotlib plots:

**For IMU data:**
- 3-panel time series (X, Y, Z axes)
- Acceleration magnitude plot
- Distribution histograms
- Multi-device comparisons (if multiple devices)

**For PPG data:**
- Time series of PPG signal
- Value distribution
- Device-to-device comparison

**For all types:**
- Box plots by device
- Summary statistics
- Timeline views

## Tips

### Performance

- Use `--since` instead of `--start/--end` when possible (simpler query)
- Add `--limit` to BigQuery queries for large datasets (modify the command)
- Parquet format is faster than CSV for large exports

### Data Quality

- Check `/sensor-explore` first to understand data freshness
- Use `/sensor-devices --sort recent` to find active devices
- Filter by `--since 7d` to focus on recent, quality data

### Working with Results

After running `/sensor-data`, the DataFrame is available:

```python
# Filter to specific time range
recent = unpacked_df[unpacked_df['timestamp'] > '2026-01-15']

# Aggregate by device
per_device = unpacked_df.groupby('device_id').agg({
    'value': ['mean', 'std', 'min', 'max']
})

# Apply custom analysis
from verily.raw_data_tools.transforms import apply_to_dataframe

def custom_analysis(df):
    df['rolling_mean'] = df['value'].rolling(window=100).mean()
    return df

analyzed = apply_to_dataframe(custom_analysis)(unpacked_df)
```

## Troubleshooting

**"No Data Collections found"**
- Check you have Data Collections attached to your workspace
- Specify manually with `--collection <name>`
- Use Workbench UI to attach a Data Collection

**"No data found"**
- Check date range - data might be outside your `--since` window
- Verify device IDs exist: `/sensor-devices`
- Check data type spelling: `/sensor-explore`

**"Module not found: verily.raw_data_tools"**
- Install the library: `pip install verily-raw-data-tools`

**Slow queries**
- Narrow date range with `--since 1d` or `--start/--end`
- Filter by specific devices with `--devices`
- Add `LIMIT` clause to BigQuery queries (modify command SQL)

## Notes

- Commands automatically handle Workbench authentication (no config needed)
- All visualizations use matplotlib (not GUIs) - data scientists stay in control
- Data never leaves Workbench - all processing is in your secure workspace
- Results are standard pandas DataFrames - use with any Python data science tools
- Compatible with JupyterLab, VS Code, and RStudio in Workbench cloud apps

## Related Documentation

- [verily-raw-data-tools README](../README.md)
- [Verily Workbench Docs](https://support.workbench.verily.com/docs/)
- [Workbench BigQuery Guide](https://support.workbench.verily.com/tags/bigquery/)
