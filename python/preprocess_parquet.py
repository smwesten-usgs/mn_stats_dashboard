import sys
import pandas as pd
from pathlib import Path

def replace_bogus_huc_with_label(huc10):

    value=huc10
    if huc10=='0000000001':
        value="State_of_Minnesota"
    return value

def read_and_add_diff_column(parquet_path: str, verbose: bool = True) -> pd.DataFrame:
    df = pd.read_parquet(parquet_path)

    if verbose:
        print("Loaded DataFrame:")
        print(df.head())
        print("\nUnique time_periods:", df['time_period'].unique())
        print("Unique summary_basetype values:", df['summary_basetype'].unique())

    # Filter to relevant time periods
    relevant_periods = ['1995-2014', '2040-2059', '2080-2099']
    df_filtered = df[df['time_period'].isin(relevant_periods)].copy()

    # Extract historical rows (no diff, just original mean)
    historical_rows = df_filtered[
        (df_filtered['scenario_name'] == 'historical') &
        (df_filtered['time_period'] == '1995-2014')
    ].copy()
    historical_rows['diff'] = pd.NA  # No diff for historical

    if verbose:
        print("\nFiltered to relevant time periods:")
        print(df_filtered['time_period'].value_counts())

    # Prepare a list to collect diff results
    diff_results = []

    # Define base grouping columns
    base_group_cols = [
        'zone', 'summary_basetype', 'swb_variable_name', 'weather_data_name'
    ]

    for basetype in df_filtered['summary_basetype'].dropna().unique():
        df_sub = df_filtered[df_filtered['summary_basetype'] == basetype].copy()

        group_cols = base_group_cols.copy()
        if basetype == 'mean_monthly':
            df_sub = df_sub.dropna(subset=['month'])
            group_cols.append('month')
        elif basetype == 'mean_seasonal':
            df_sub = df_sub.dropna(subset=['season_name'])
            group_cols.append('season_name')

        df_sub = df_sub.dropna(subset=group_cols + ['mean'])

        if verbose:
            print(f"\nProcessing summary_basetype: {basetype}")
            print(f"   Grouping columns: {group_cols}")
            print(f"   Rows after dropna: {len(df_sub)}")

        # Split into baseline and future
        baseline_df = df_sub[
            (df_sub['scenario_name'] == 'historical') &
            (df_sub['time_period'] == '1995-2014')
        ][group_cols + ['mean']].rename(columns={'mean': 'baseline_mean'})

        future_df = df_sub[df_sub['scenario_name'] != 'historical'].copy()

        if verbose:
            print("\nBaseline DataFrame:")
            print(baseline_df.head())
            print("\nFuture DataFrame:")
            print(future_df[['scenario_name', 'time_period'] + group_cols + ['mean']].head())

        # Merge baseline into future
        merged = future_df.merge(baseline_df, on=group_cols, how='left')

        # Compute diff
        merged['diff'] = merged['mean'] - merged['baseline_mean']

        if verbose:
            print("\nMerged DataFrame with diff:")
            print(merged[['scenario_name', 'time_period'] + group_cols + ['mean', 'baseline_mean', 'diff']].head())

        diff_results.append(merged)

    # Combine all diff results
    all_diffs = pd.concat(diff_results, ignore_index=True)

    if verbose:
        print("\nCombined diff DataFrame:")
        print(all_diffs.head())
        print(f"Total rows in diff table: {len(all_diffs)}")


    # Combine with future rows that have diffs
    final_df = pd.concat([all_diffs, historical_rows], ignore_index=True)

    # pad HUC numbers with leading zeros
    final_df['huc10'] = [replace_bogus_huc_with_label(s.zfill(10)) for s in final_df.zone]

    return final_df

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python your_script.py <path_to_parquet_file>")
        sys.exit(1)

    parquet_file = Path(sys.argv[1])

    try:
        df_with_diff = read_and_add_diff_column(parquet_file, verbose=True)
        print(df_with_diff.head())  # Show a preview
        df_with_diff.to_parquet(f"data/{parquet_file.stem}_w_diff.parquet")
    except Exception as e:
        print(f"Error processing file: {e}")
        sys.exit(1)
