import pandas as pd
import panel as pn
import sys
import datetime
import geopandas as gpd
import holoviews as hv
from holoviews import opts
from pathlib import Path
import folium

# Initialize Panel extension
pn.extension('tabulator',
             'floatpanel',
             raw_css=[
                 """
                 body {
                     background-color: #e6e6e6;
                 }
                 """
             ],
)

hv.extension('bokeh')

data_dir = Path('data')

data_file = data_dir / Path('merged_swb_output__all_output_w_diff.parquet')
try:
    df = pd.read_parquet(data_file)
except Exception as e:
    print(f"Error processing file: {e}")
    sys.exit(1)

# Load the shapefile
shapefile_path = data_dir / 'HUC_10_selections_MN_SWB.shp'
huc_data = gpd.read_file(shapefile_path)


def get_monthname(monthnum):

    month = datetime.date(1900, monthnum, 1).strftime('%B')
    return month


def replace_bogus_huc_with_label(huc10):

    value=huc10
    if huc10=='0000000001':
        value="State_of_Minnesota"
    return value

def mean_values_for_pivot_table(df):

    df = df.reset_index()

    filtered_prism_df =  df[(df['weather_data_name'] == 'prism')]
    filtered_non_prism_df =  df[(df['weather_data_name'] != 'prism')]

    # Compute mean of numeric columns
    mean_values = filtered_non_prism_df.select_dtypes(include='number').mean().round(2)

    n = 0
    for val in mean_values:
        n+=1
        print(f"{n}) {val}")
    
    #df = df.reset_index()

    print("__________________________________")
    print(['AVERAGE OF MODELS'] + mean_values.tolist())
    print("__________________________________")
    print(filtered_non_prism_df)
    print("__________________________________")
    print(filtered_non_prism_df.columns)

    # Create a new row with label 'mean' and the rest as the computed means
    mean_row = pd.DataFrame([['AVERAGE OF MODELS'] + mean_values.tolist()], columns=filtered_non_prism_df.columns)
    #print(mean_row)
    # Concatenate the mean row with the original DataFrame
    df_with_mean = pd.concat([filtered_non_prism_df, mean_row, filtered_prism_df], ignore_index=True)
    return df_with_mean

def create_huc10_info(huc10_id):
    filtered_df = huc_data[huc_data['huc10'] == huc10_id]
    
    try:
        #description_txt = filtered_df.Station_Name
        description_txt = (f"# {str(filtered_df['name'].values[0])}\n")
                           
    except:
        description_txt = "no selection"

    if huc10_id == '0000000001':
        description_txt = "# State of Minnesota"        

    static_text = pn.pane.Markdown(description_txt, hard_line_break=True)
    return static_text


def filter_data_by_selection(
    df,
    summary_basetype,
    huc10=None,
    swb_variable_name=None,
    season_name=None,
    month=None,
    diff_button=False
):
    df_sub = df[df['summary_basetype'] == summary_basetype].copy()

    if huc10:
        df_sub = df_sub[df_sub['huc10'] == huc10]
    if swb_variable_name:
        df_sub = df_sub[df_sub['swb_variable_name'] == swb_variable_name]

    # Conditional filters based on summary_basetype
    if summary_basetype == 'mean_seasonal' and season_name:
        df_sub = df_sub[df_sub['season_name'] == season_name]
    elif summary_basetype == 'mean_monthly':
        if month is not None:
            df_sub = df_sub[df_sub['month'] == month]

    return df_sub







#@pn.depends(huc10=huc10_selector.param.value,
#            swb_variable_name=swb_variable_name_selector.param.value,
#            season_name=season_selector.param.value,            
#            diff_button=diff_button.param.value)
def update_table(filtered_df, summary_basetype, time_period, huc10, swb_variable_name, season_name, month, diff_button):
    filtered_cmip6_df =  filtered_df[((filtered_df['time_period']=='1995-2014') | (filtered_df['time_period']==time_period))] # &
                                     #(filtered_df['weather_data_name'] != 'prism')]

    #filtered_prism_df =  filtered_df[(filtered_df['weather_data_name'] == 'prism')]

    match summary_basetype:
        case 'mean_annual':
            title_object_txt = f"mean_annual__{time_period}"
        case 'mean_seasonal':
            title_object_txt = f"mean_seasonal_{season_name}__{time_period}"
        case 'mean_growing-season':
            title_object_txt = f"mean_growing_season__{time_period}"
        case 'mean_monthly':
            title_object_txt = f"mean_monthly_{month}__{time_period}"

    values='mean'
    if diff_button:
        values='diff'
    output_filename = f"{values}_{swb_variable_name}_for_{replace_bogus_huc_with_label(huc10)}_{title_object_txt}.csv"

    pivot_df = filtered_cmip6_df.pivot_table(
                    index='weather_data_name',
                    columns='scenario_name',
                    values=values).round(2)

    pivot_w_mean = mean_values_for_pivot_table(pivot_df)

    pivot_tab = pn.widgets.Tabulator(pivot_w_mean, layout='fit_data_table', show_index=False)

    filename_tab, button_tab = pivot_tab.download_menu(
                                   text_kwargs={'name': 'Enter filename', 'value': output_filename},
                                   button_kwargs={'name': 'Download table'}
    )

    return pn.Column(pivot_tab, pn.Column(filename_tab, button_tab))









def update_plot(filtered_df, summary_basetype, time_period, huc10, swb_variable_name, season_name, month, diff_button):
    
    grid_style = {'grid_line_color': 'black', 'grid_line_width': 1.0, # 'ygrid_bounds': (0.3, 0.7),
              'xgrid_line_color': 'lightgray', 'xgrid_line_dash': [4, 4]}

    filtered_df =  filtered_df[(filtered_df['time_period']=='1995-2014') | (filtered_df['time_period']==time_period)]

    match summary_basetype:
        case 'mean_annual':
            title_object_txt = f"mean annual"
        case 'mean_seasonal':
            title_object_txt = f"mean seasonal ({season_name})"
        case 'mean_growing-season':
            title_object_txt = f"mean growing season"
        case 'mean_monthly':
            title_object_txt = f"mean monthly ({month})"

    if diff_button:
        vdims='diff'
        title_prefix=f"projections, compared to historical: {title_object_txt}"
        ylabel='Mean Difference'
        # remove scenario_name of 'historical' from dataframe
        filtered_df = filtered_df[(filtered_df['scenario_name']!='historical')]
        colormap = ['forestgreen','gold','firebrick']
        ymin = filtered_df['diff'].min()
        ymax = filtered_df['diff'].max()
    else:
        vdims='mean'
        title_prefix=f"projections: {title_object_txt}"
        ylabel='Mean Value'
        colormap = ['forestgreen','gold','firebrick','royalblue']
        ymin = filtered_df['mean'].min()
        ymax = filtered_df['mean'].max()

    if time_period=='2040-2059':
        title_txt = f"Mid-century {title_prefix} (2040-2059)"    
    else:
        title_txt = f"Late-century {title_prefix} (2080-2099)"

    bars = hv.Bars(filtered_df, kdims=['weather_data_name','scenario_name'], vdims=[vdims]).opts(
        ylim=(ymin, ymax),
        framewise=True,
        title=title_txt,
        xlabel='Model Name',
        ylabel=ylabel,
        tools=['hover'],
        width=700,
        height=450,
        color='scenario_name',  # Use scenario_name for color differentiation
        cmap=colormap,
        show_legend=False,
        #legend_position='right',
        gridstyle=grid_style,
        show_grid=True,
        xrotation=45
    )

    # return bars.redim.range(y=(ymin, ymax))
    # return bars.redim(y=hv.Dimension('y', range=(ymin, ymax)))
    return bars.redim.range(value=(ymin, ymax))

# Create widgets for filtering
huc10_selector = pn.widgets.Select(name='HUC 10', options=list(df['huc10'].unique()), value=None)
swb_variable_name_selector = pn.widgets.Select(name='SWB Variable Name', options=list(df['swb_variable_name'].unique()), value=None)
season_selector = pn.widgets.Select(name='Season', options=list(df['season_name'].unique()), value=None)
diff_button = pn.widgets.Toggle(name='Compare to historical', button_type='default')
summary_selector = pn.widgets.Select(
    name='Summary Type',
    options=['mean_annual', 'mean_seasonal', 'mean_monthly', 'mean_growing-season'],
    value='mean_seasonal'
)
month_selector = pn.widgets.IntSlider(name='Month', start=1, end=12, value=1)


filtered_df = pn.bind(
    filter_data_by_selection,
    df=df,
    summary_basetype=summary_selector,
    huc10=huc10_selector,
    swb_variable_name=swb_variable_name_selector,
    season_name=season_selector,
    month=month_selector,
    diff_button=diff_button
)

bound_plot_2040 = pn.bind(
    update_plot,
    filtered_df,
    summary_basetype=summary_selector,
    time_period='2040-2059',
    huc10=huc10_selector,
    swb_variable_name=swb_variable_name_selector,
    season_name=season_selector,
    month=month_selector,
    diff_button=diff_button
)

bound_plot_2080 = pn.bind(
    update_plot,
    filtered_df,
    summary_basetype=summary_selector,
    time_period='2080-2099',
    huc10=huc10_selector,
    swb_variable_name=swb_variable_name_selector,
    season_name=season_selector,
    month=month_selector,
    diff_button=diff_button
)

bound_table_2040 = pn.bind(
    update_table,
    filtered_df,
    summary_basetype=summary_selector,
    time_period='2040-2059',
    huc10=huc10_selector,
    swb_variable_name=swb_variable_name_selector,
    season_name=season_selector,
    month=month_selector,
    diff_button=diff_button
)

bound_table_2080 = pn.bind(
    update_table,
    filtered_df,
    summary_basetype=summary_selector,
    time_period='2080-2099',
    huc10=huc10_selector,
    swb_variable_name=swb_variable_name_selector,
    season_name=season_selector,
    month=month_selector,
    diff_button=diff_button
)

@pn.depends(huc_id=huc10_selector.param.value)
def update_huc10_info(huc_id):
    return create_huc10_info(huc_id)

@pn.depends(huc_id=huc10_selector.param.value)
def update_map(huc_id):
    try:
        # Filter the GeoDataFrame for the selected HUC
        selected_huc_data = huc_data[huc_data['huc10'] == huc_id]
        # Get the centroid for placing the map
        centroid = selected_huc_data.geometry.centroid.to_crs(epsg=4326)
        # Use WGS 84 (epsg:4326) as the geographic coordinate system
        # folium (i.e. leaflet.js) by default accepts values of latitude and longitude (angular units) as input;
        # we need to project the geometry to a geographic coordinate system first.
        selected_huc_data = selected_huc_data.to_crs(epsg=4326)
        print('Selected (reprojected) data:')
        print(selected_huc_data)
        map_center = [centroid.y.mean(), centroid.x.mean()]
        # Create a folium map
        m = folium.Map(location=map_center, zoom_start=10, tiles='OpenStreetMap')

        for _, r in selected_huc_data.iterrows():
            geo_j = gpd.GeoSeries(r["geometry"]).to_json()
            geo_j = folium.GeoJson(data=geo_j, style_function=lambda x: {"fillColor": "orange"})
            folium.Popup(r["name"]).add_to(geo_j)
            geo_j.add_to(m)
            
        m.fit_bounds(m.get_bounds(), padding=(30, 30))
    except:
        print(f"Something went wrong generating the HUC map. Displaying a generic map.")
        print(f"  huc_id = {huc_id}")
        print(f"selected_huc_data = {selected_huc_data}")
        m = folium.Map(location=[42, -96], zoom_start=10, tiles="OpenStreetMap")

    return m

# Layout the dashboard
dashboard = pn.GridSpec(sizing_mode='stretch_both', max_height=1000)
dashboard[0, 0:2] = summary_selector
dashboard[1, 0:2] = huc10_selector
dashboard[2, 0:2] = swb_variable_name_selector
dashboard[3, 0:1] = season_selector
dashboard[4, 0:1] = month_selector
dashboard[5, 0:1] = diff_button
dashboard[0, 2:21] = update_huc10_info

dashboard[1:7,2:11] =pn.Column(bound_plot_2040,
                               sizing_mode='stretch_both')
dashboard[1:7,11:20] =pn.Column(bound_plot_2080,
                                sizing_mode='stretch_both')

dashboard[9:14,11:20] =update_map
dashboard[9:14,0:5] =pn.Column(pn.pane.Markdown("### Mid-century projections (2040-2059)"),
                               bound_table_2040
)
dashboard[9:14,5:10] =pn.Column(pn.pane.Markdown("### Late-century projections (2080-2099)"),
                                bound_table_2080
)
# Serve the dashboard
dashboard.servable()