import pandas as pd
import numpy as np
import holoviews as hv
from pathlib import Path
import sys
import panel as pn

hv.extension('bokeh')
pn.extension()

# Dummy data generator
def make_df(scale):
    models = ['Model A', 'Model B', 'Model C']
    scenarios = ['low', 'medium', 'high']
    data = []
    for model in models:
        for scenario in scenarios:
            value = np.random.rand() * scale
            data.append((model, scenario, value))
    return pd.DataFrame(data, columns=['weather_data_name', 'scenario_name', 'mean'])

data_dir = Path('data')

data_file = data_dir / Path('merged_swb_output__all_output_w_diff.parquet')
try:
    df = pd.read_parquet(data_file)
except Exception as e:
    print(f"Error processing file: {e}")
    sys.exit(1)


def filter_data_by_selection(
    df,
    summary_basetype,
    huc10=None,
    time_period='2040-2059',
    swb_variable_name=None,
    season_name=None,
    month=None,
    diff_button=False
):
    df_sub = df[df['summary_basetype'] == summary_basetype].copy()

    if huc10:
        df_sub = df_sub[df_sub['huc10'] == huc10]
    if time_period:
        df_sub = df_sub[df_sub['time_period'] == time_period]
    if swb_variable_name:
        df_sub = df_sub[df_sub['swb_variable_name'] == swb_variable_name]

    # Conditional filters based on summary_basetype
    if summary_basetype == 'mean_seasonal' and season_name:
        df_sub = df_sub[df_sub['season_name'] == season_name]
    elif summary_basetype == 'mean_monthly':
        if month is not None:
            df_sub = df_sub[df_sub['month'] == month]

    return df_sub

# Plotting function
def update_plot(monthnum):
    df_sub = filter_data_by_selection(df, 'mean_monthly', '0401020204', '2040-2059', 'net_infiltration', None, monthnum, False,)
    ymin, ymax = df_sub['mean'].min(), df_sub['mean'].max()
    
    bars = hv.Bars(df_sub, kdims=['weather_data_name', 'scenario_name'], vdims=['mean']).opts(
        xlabel='Model',
        ylabel='Mean Value',
        tools=['hover'],
        width=600,
        height=400,
        color='scenario_name',
        cmap=['forestgreen', 'gold', 'firebrick'],
        show_legend=False,
        xrotation=45,
        gridstyle={'grid_line_color': 'lightgray'},
    )
    
    bars = bars.clone()
    return bars.redim.range(mean=(ymin, ymax))

# Interactive widget
month_slider = pn.widgets.IntSlider(name='Month', start=1, end=12, value=1)

# Bind and layout
bound_plot = pn.bind(update_plot, month_slider)


grid = pn.GridSpec(sizing_mode='stretch_both', max_height=600)
grid[0:6, 0:12] = pn.panel(bound_plot, sizing_mode='stretch_both')
grid[6, 0:12] = month_slider

grid.servable()



# layout = pn.Column(
#     pn.pane.Markdown("### Dynamic Y-Axis Test"),
#     pn.panel(bound_plot, sizing_mode='stretch_width'),
#     month_slider,
#     sizing_mode='stretch_both'
# )

# layout.servable()
