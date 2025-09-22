import sys
from pathlib import Path
import pandas as pd

def read_and_add_diff_column(parquet_path: str) -> pd.DataFrame:
    df = pd.read_parquet(parquet_path)

    # Filter to relevant time periods
    relevant_periods = ['1995-2014', '2040-2059', '2080-2099']
    df_filtered = df[df['time_period'].isin(relevant_periods)].copy()

    # Prepare a list to collect diff results
    diff_results = []

    # Define base grouping columns
    base_group_cols = [
        'summary_basetype', 'scenario_name',
        'swb_variable_name', 'weather_data_name'
    ]

    # Process each summary_basetype separately
    for basetype in df_filtered['summary_basetype'].dropna().unique():
        df_sub = df_filtered[df_filtered['summary_basetype'] == basetype].copy()

        # Determine additional grouping columns
        group_cols = base_group_cols.copy()
        if basetype == 'mean_monthly':
            df_sub = df_sub.dropna(subset=['month'])
            group_cols.append('month')
        elif basetype == 'mean_seasonal':
            df_sub = df_sub.dropna(subset=['season_name'])
            group_cols.append('season_name')

        # Drop rows with missing values in grouping columns or 'mean'
        df_sub = df_sub.dropna(subset=group_cols + ['mean'])

        # Pivot to wide format
        pivot_df = (
            df_sub
            .pivot_table(index=group_cols, columns='time_period', values='mean')
            .reset_index()
        )

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

        # Map diff_period to time_period
        diff_long['time_period'] = diff_long['diff_period'].map({
            'diff_2040_2059': '2040-2059',
            'diff_2080_2099': '2080-2099'
        })

        diff_long = diff_long.drop(columns='diff_period')
        diff_results.append(diff_long)

    # Combine all diff results
    all_diffs = pd.concat(diff_results, ignore_index=True)

    # Merge back into original DataFrame
    df = df.merge(all_diffs, on=base_group_cols + ['time_period'] + ['month', 'season_name'], how='left')

    return df


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python your_script.py <path_to_parquet_file>")
        sys.exit(1)

    parquet_file = Path(sys.argv[1])

    try:
        df_with_diff = read_and_add_diff_column(parquet_file)
        print(df_with_diff.head())  # Show a preview
        df_with_diff.to_parquet(f"data/{parquet_file.stem}_w_diff.parquet")
    except Exception as e:
        print(f"Error processing file: {e}")
        sys.exit(1)
