import pandas as pd
import panel as pn
import sys
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

def replace_bogus_huc_with_label(huc10):

    value=huc10
    if huc10=='0000000001':
        value="State_of_Minnesota"
    return value


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

# Create a function to filter the DataFrame based on user input
def filter_data_by_selection(huc10, swb_variable_name, season_name):
    filtered_df = df.copy()
    
    if huc10:
        filtered_df = filtered_df[filtered_df['huc10'] == huc10]
    if swb_variable_name:
        filtered_df = filtered_df[filtered_df['swb_variable_name'] == swb_variable_name]
    if season_name:
        filtered_df = filtered_df[filtered_df['season_name'] == season_name]
    
    return filtered_df

# Create widgets for filtering
huc10_selector = pn.widgets.Select(name='HUC 10', options=list(df['huc10'].unique()), value=None)
swb_variable_name_selector = pn.widgets.Select(name='SWB Variable Name', options=list(df['swb_variable_name'].unique()), value=None)
season_selector = pn.widgets.Select(name='Season', options=list(df['season_name'].unique()), value=None)
diff_button = pn.widgets.Toggle(name='Compare to historical', button_type='default')

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

@pn.depends(huc10=huc10_selector.param.value,
            swb_variable_name=swb_variable_name_selector.param.value,
            season_name=season_selector.param.value,            
            diff_button=diff_button.param.value)
def update_mid_table(huc10, swb_variable_name, season_name, diff_button):
    filtered_df = filter_data_by_selection(huc10, swb_variable_name, season_name)
    filtered_mid_century_df =  filtered_df[(filtered_df['time_period']=='1995-2014') | (filtered_df['time_period']=='2040-2059')]
    
    values='mean'
    if diff_button:
        values='diff'
    output_filename = f"{values}_{swb_variable_name}_for_{replace_bogus_huc_with_label(huc10)}_{season_name}_2040-2059.csv"

    pivot_mid_df = filtered_mid_century_df.pivot_table(
                    index='weather_data_name',
                    columns='scenario_name',
                    values=values).round(2)

    pivot_mid_tab = pn.widgets.Tabulator(pivot_mid_df, layout='fit_data_table', show_index=True)

    filename_mid, button_mid = pivot_mid_tab.download_menu(
                                   text_kwargs={'name': 'Enter filename', 'value': output_filename},
                                   button_kwargs={'name': 'Download table'}
    )

    return pn.Column(pivot_mid_tab, pn.Column(filename_mid, button_mid))


@pn.depends(huc10=huc10_selector.param.value,
            swb_variable_name=swb_variable_name_selector.param.value,
            season_name=season_selector.param.value,            
            diff_button=diff_button.param.value)
def update_late_table(huc10, swb_variable_name, season_name, diff_button):
    filtered_df = filter_data_by_selection(huc10, swb_variable_name, season_name)
    filtered_late_century_df = filtered_df[(filtered_df['time_period']=='1995-2014') | (filtered_df['time_period']=='2080-2099')]

    values='mean'
    if diff_button:
        values='diff'
    output_filename = f"{values}_{swb_variable_name}_for_{replace_bogus_huc_with_label(huc10)}_{season_name}_2080-2099.csv"

    pivot_late_df = filtered_late_century_df.pivot_table(
                    index='weather_data_name',
                    columns='scenario_name',
                    values=values).round(2)

    pivot_late_tab = pn.widgets.Tabulator(pivot_late_df, layout='fit_data_table', show_index=True)

    filename_late, button_late = pivot_late_tab.download_menu(
                                   text_kwargs={'name': 'Enter filename', 'value': output_filename},
                                   button_kwargs={'name': 'Download table'}
    )

    return pn.Column(pivot_late_tab, pn.Column(filename_late, button_late))


# Create a function to update the plot based on the selected filters
@pn.depends(huc10=huc10_selector.param.value,
            swb_variable_name=swb_variable_name_selector.param.value,
            season_name=season_selector.param.value,
            diff_button=diff_button.param.value)
def update_mid_century_plot(huc10, swb_variable_name, season_name, diff_button):
    filtered_df = filter_data_by_selection(huc10, swb_variable_name, season_name)
    filtered_mid_century_df = filtered_df[(filtered_df['time_period']=='1995-2014') | (filtered_df['time_period']=='2040-2059')]
    
    grid_style = {'grid_line_color': 'black', 'grid_line_width': 1.0, # 'ygrid_bounds': (0.3, 0.7),
              'xgrid_line_color': 'lightgray', 'xgrid_line_dash': [4, 4]}

    if diff_button:
        vdims='diff'
        title_prefix=f"projections, compared to historical: {season_name}"
        ylabel='Mean Difference'
        # remove scenario_name of 'historical' from dataframe
        filtered_mid_century_df = filtered_mid_century_df[(filtered_mid_century_df['scenario_name']!='historical')]
        colormap = ['forestgreen','gold','firebrick']
    else:
        vdims='mean'
        title_prefix=f"projections: {season_name}"
        ylabel='Mean Value'
        colormap = ['royalblue','forestgreen','gold','firebrick']

    title_txt_mid = f"Mid-century {title_prefix} (2040-2059)"

    # Create a grouped bar plot
    bars_mid = hv.Bars(filtered_mid_century_df, kdims=['weather_data_name','scenario_name'], vdims=[vdims]).opts(
        title=title_txt_mid,
        xlabel='Model Name',
        ylabel=ylabel,
        tools=['hover'],
        width=700,
        height=450,
        color='scenario_name',  # Use scenario_name for color differentiation
        cmap=colormap,#'Category10',  # Use a categorical color map
        show_legend=False,
        #legend_position='top',
        gridstyle=grid_style,
        show_grid=True,
        xrotation=45
    ).redim.range(y=(None, None))

    # bars2 = hv.Bars(filtered_late_century_df, kdims=['weather_data_name','scenario_name'], vdims=[vdims]).opts(
    #     title=title_txt_late,
    #     xlabel='Model Name',
    #     ylabel=ylabel,
    #     tools=['hover'],
    #     width=850,
    #     height=500,
    #     color='scenario_name',  # Use scenario_name for color differentiation
    #     cmap=colormap,
    #     show_legend=True,
    #     legend_position='right',
    #     gridstyle=grid_style,
    #     show_grid=True,
    #     xrotation=45
    # )

    return bars_mid# + bars2)



# Create a function to update the plot based on the selected filters
@pn.depends(huc10=huc10_selector.param.value,
           swb_variable_name=swb_variable_name_selector.param.value,
           season_name=season_selector.param.value,
           diff_button=diff_button.param.value)
def update_late_century_plot(huc10, swb_variable_name, season_name, diff_button):
    filtered_df = filter_data_by_selection(huc10, swb_variable_name, season_name)
    filtered_late_century_df = filtered_df[(filtered_df['time_period']=='1995-2014') | (filtered_df['time_period']=='2080-2099')]
    
    grid_style = {'grid_line_color': 'black', 'grid_line_width': 1.0, # 'ygrid_bounds': (0.3, 0.7),
              'xgrid_line_color': 'lightgray', 'xgrid_line_dash': [4, 4]}

    if diff_button:
        vdims='diff'
        title_prefix=f"projections, compared to historical: {season_name}"
        ylabel='Mean Difference'
        # remove scenario_name of 'historical' from dataframe
        filtered_late_century_df = filtered_late_century_df[(filtered_late_century_df['scenario_name']!='historical')]
        colormap = ['forestgreen','gold','firebrick']
        ymin = filtered_late_century_df['diff'].min()
        ymax = filtered_late_century_df['diff'].max()
    else:
        vdims='mean'
        title_prefix=f"projections: {season_name}"
        ylabel='Mean Value'
        colormap = ['royalblue','forestgreen','gold','firebrick']
        ymin = filtered_late_century_df['mean'].min()
        ymax = filtered_late_century_df['mean'].max()


    title_txt_late = f"Late-century {title_prefix} (2080-2099)"

    bars_late = hv.Bars(filtered_late_century_df, kdims=['weather_data_name','scenario_name'], vdims=[vdims]).opts(
        ylim=(ymin, ymax),
        framewise=True,
        title=title_txt_late,
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
    ).redim.range(y=(None, None))

    return bars_late

# Layout the dashboard
dashboard = pn.GridSpec(sizing_mode='stretch_both', max_height=1000)
dashboard[0, 0:2] = swb_variable_name_selector
dashboard[1, 0:2] = huc10_selector
dashboard[2, 0:1] = season_selector
dashboard[3, 0:1] = diff_button
dashboard[0, 2:21] = update_huc10_info
dashboard[1:7,2:11] =pn.Column(update_mid_century_plot,
                               sizing_mode='stretch_both')
dashboard[1:7,11:20] =pn.Column(update_late_century_plot,
                               sizing_mode='stretch_both')
dashboard[9:14,11:20] =update_map
dashboard[9:14,0:5] =pn.Column(pn.pane.Markdown("### Mid-century projections (2040-2059)"),
                               update_mid_table
)
dashboard[9:14,5:10] =pn.Column(pn.pane.Markdown("### Late-century projections (2080-2099)"),
                               update_late_table
)
# Serve the dashboard
dashboard.servable()