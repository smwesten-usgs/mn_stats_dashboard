import sys
import pandas as pd
from pathlib import Path

parquet_path = Path("data/merged_swb_output__all_output.parquet")
verbose = True

#def read_and_add_diff_column(parquet_path: str, verbose: bool = True) -> pd.DataFrame:
df = pd.read_parquet(parquet_path)

if verbose:
    print("Loaded DataFrame:")
    print(df.head())
    print("\nUnique time_periods:", df['time_period'].unique())
    print("Unique summary_basetype values:", df['summary_basetype'].unique())

# Filter to relevant time periods
relevant_periods = ['1995-2014', '2040-2059', '2080-2099']
df_filtered = df[df['time_period'].isin(relevant_periods)].copy()

if verbose:
    print("\nFiltered to relevant time periods:")
    print(df_filtered['time_period'].value_counts())

# Split baseline and future
baseline_df = df_filtered[df_filtered['scenario_name'] == 'historical']
future_df = df_filtered[df_filtered['scenario_name'] != 'historical']

# Prepare a list to collect diff results
diff_results = []

# Define base grouping columns
base_group_cols = [
    'zone',
    'summary_basetype', 'scenario_name',
    'swb_variable_name', 'weather_data_name'
]

#for basetype in df_filtered['summary_basetype'].dropna().unique():

basetype = 'mean_annual'
df_sub = df_filtered[df_filtered['summary_basetype'] == basetype].copy()

group_cols = base_group_cols.copy()
if basetype == 'mean_monthly':
    df_sub = df_sub.dropna(subset=['month'])
    group_cols.append('month')
elif basetype == 'mean_seasonal':
    df_sub = df_sub.dropna(subset=['season_name'])
    group_cols.append('season_name')

# drop any rows for which our group_cols or the mean value is NaN
df_sub = df_sub.dropna(subset=group_cols + ['mean'])

if verbose:
    print(f"\nProcessing summary_basetype: {basetype}")
    print(f"   Grouping columns: {group_cols}")
    print(f"   Rows after dropna: {len(df_sub)}")

# Pivot to wide format
pivot_df = (
    df_sub
    .pivot_table(index=group_cols, columns='time_period', values='mean')
    .reset_index()
)

if verbose:
    print("\nPivoted DataFrame:")
    print(pivot_df.head())

# Calculate diffs
pivot_df['diff_2040_2059'] = pivot_df.get('2040-2059') - pivot_df.get('1995-2014')
pivot_df['diff_2080_2099'] = pivot_df.get('2080-2099') - pivot_df.get('1995-2014')

# Melt back to long format
diff_long = pivot_df.melt(
    id_vars=group_cols,
    value_vars=['diff_2040_2059', 'diff_2080_2099'],
    var_name='diff_period',
    value_name='diff'
)

diff_long['time_period'] = diff_long['diff_period'].map({
    'diff_2040_2059': '2040-2059',
    'diff_2080_2099': '2080-2099'
})

diff_long = diff_long.drop(columns='diff_period')

if verbose:
    print("\n📎 Melted diff DataFrame:")
    print(diff_long.head())

diff_results.append((group_cols + ['time_period'], diff_long))

# combine all diff results
for merge_keys, diff_df in diff_results:
    df = df.merge(diff_df, on=merge_keys, how='left')

# Combine all diff results
## all_diffs = pd.concat(diff_results, ignore_index=True)

if verbose:
    print("\nCombined diff DataFrame:")
    print(all_diffs.head())
    print(f"Total rows in diff table: {len(all_diffs)}")

# Merge back into original DataFrame
df = df.merge(all_diffs, on=group_cols + ['time_period', 'month', 'season_name'], how='left')

if verbose:
    print("\nFinal merged DataFrame preview:")
    print(df[['summary_basetype', 'time_period', 'month', 'season_name', 'mean', 'diff']].head(10))
    print(f"Non-null diff values: {df['diff'].notna().sum()} / {len(df)}")

#return df

# if __name__ == "__main__":
#     if len(sys.argv) != 2:
#         print("Usage: python your_script.py <path_to_parquet_file>")
#         sys.exit(1)

#     parquet_file = Path(sys.argv[1])

#     try:
#         df_with_diff = read_and_add_diff_column(parquet_file, verbose=True)
#         print(df_with_diff.head())  # Show a preview
#         df_with_diff.to_parquet(f"data/{parquet_file.stem}_w_diff.parquet")
#     except Exception as e:
#         print(f"Error processing file: {e}")
#         sys.exit(1)
