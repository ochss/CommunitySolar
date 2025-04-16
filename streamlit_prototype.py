import streamlit as st
import sqlite3
import pandas as pd
import folium
from streamlit_folium import st_folium
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode

st.set_page_config(layout="wide")  # Make app wide

# Data loading function with caching for performance.
@st.cache_data
def load_data():
    conn = sqlite3.connect('community_solar.db')
    query = """
    SELECT 
        LOCATIONS.location_id,
        LOCATIONS.latitude,
        LOCATIONS.longitude,
        LOCATIONS.dlgf_prop_class_code,
        LOCATIONS.orig_addr,
        LOCATIONS.geocity,
        LOCATIONS.geozip,
        GOOGLE_SOLAR.max_panel_count,
        GOOGLE_SOLAR.yearly_energy_production
    FROM 
        LOCATIONS
    INNER JOIN 
        GOOGLE_SOLAR
    ON
        GOOGLE_SOLAR.location_id = LOCATIONS.location_id 
    WHERE 
        GEOCITY = 'VALPARAISO'
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    # Convert the property code field to string.
    #df['dlgf_prop_class_code'] = df['dlgf_prop_class_code'].astype(str)
    df['dlgf_prop_class_code'] = pd.to_numeric(df['dlgf_prop_class_code'], errors='coerce')

    df['Google Maps Link'] = df.apply(
        #lambda row: f'<a href="https://www.google.com/maps/search/?api=1&query={row["latitude"]},{row["longitude"]}" target="_blank">View on Google Maps</a>', 
        lambda row: f'https://www.google.com/maps/search/?api=1&query={row["latitude"]},{row["longitude"]}', 
        axis=1
    )
    
    # Convert production values to numeric.
    df['yearly_energy_production'] = pd.to_numeric(df['yearly_energy_production'], errors='coerce')
    return df

# Load the data.
df = load_data()

st.title("Community Solar Locations in Valparaiso")

## Filter 1: Select Property Code ##
codes = sorted(df['dlgf_prop_class_code'].unique())
selected_code = st.selectbox("Select a Property Code", codes)
filtered_df = df[df['dlgf_prop_class_code'] == selected_code]

## Filter 2: Yearly Energy Production via Quartiles ##
if not filtered_df.empty:
    filtered_df = filtered_df.dropna(subset=['yearly_energy_production'])
    quartiles = filtered_df['yearly_energy_production'].quantile([0.25, 0.50, 0.75]).to_dict()
    quartile_options = {
        "All": filtered_df['yearly_energy_production'].min(),
        f"Q1 (>= {quartiles[0.25]:.2f})": quartiles[0.25],
        f"Q2 (>= {quartiles[0.50]:.2f})": quartiles[0.50],
        f"Q3 (>= {quartiles[0.75]:.2f})": quartiles[0.75]
    }
    
    selected_quartile = st.selectbox("Minimum Yearly Energy Production", list(quartile_options.keys()))
    min_yearly_energy = quartile_options[selected_quartile]
    filtered_df = filtered_df[filtered_df['yearly_energy_production'] >= min_yearly_energy]
    # Sort by yearly energy production for better visualization.
    filtered_df = filtered_df.sort_values(by='yearly_energy_production', ascending=False)
    
else:
    st.warning("No data available for the selected property code.")

## Interactive Data Preview using AgGrid ##
st.subheader("Filtered Data Preview (Click a row to show it on the map)")
gb = GridOptionsBuilder.from_dataframe(filtered_df)

# Configure AgGrid to allow single row selection.
gb.configure_selection(selection_mode="single", use_checkbox=False)

gridOptions = gb.build()

# Display the grid and capture the response.
grid_response = AgGrid(
    filtered_df,
    gridOptions=gridOptions,
    update_mode=GridUpdateMode.SELECTION_CHANGED,
    theme='blue'
)

# Check the selected row.
selected_rows = grid_response.get('selected_rows')
selected_location = None

# Check if selected_rows is a DataFrame and if it's not empty.
if isinstance(selected_rows, pd.DataFrame):
    if not selected_rows.empty:
        selected_location = selected_rows.iloc[0]  # Use .iloc[0] to get the first row as a Series.
        st.markdown("### Selected Location Details")
        st.write(selected_location)
else:
    # Fallback in case it is not a DataFrame.
    if selected_rows and len(selected_rows) > 0:
        selected_location = selected_rows[0]
        st.markdown("### Selected Location Details")
        st.write(selected_location)

## Mapping ##
lat_col = 'latitude'
lon_col = 'longitude'

if lat_col in filtered_df.columns and lon_col in filtered_df.columns:
    # Remove rows with missing coordinates and convert to float.
    filtered_df = filtered_df.dropna(subset=[lat_col, lon_col])
    filtered_df[lat_col] = filtered_df[lat_col].astype(float)
    filtered_df[lon_col] = filtered_df[lon_col].astype(float)

    # Set default map center to the average of available points.
    default_center = [filtered_df[lat_col].mean(), filtered_df[lon_col].mean()]

    # If a row was selected, update the map center.
    if selected_location is not None:
        default_center = [selected_location[lat_col], selected_location[lon_col]]
    
    m = folium.Map(location=default_center, zoom_start=12)
    
    # Add markers to the map.
    for idx, row in filtered_df.iterrows():
        # Use a different marker color for the selected row.
        if selected_location is not None and row[lat_col] == selected_location[lat_col] and row[lon_col] == selected_location[lon_col]:
            popup_html = "<b>Selected Location</b><br>"
            for col in filtered_df.columns:
                if col == "Google Maps Link":
                    popup_html += f'<a href="{row[col]}" target="_blank">View on Google Maps</a><br>'
                else:
                    popup_html += f"<b>{col}:</b> {row[col]}<br>"
            folium.Marker(
                location=[row[lat_col], row[lon_col]],
                popup=popup_html,
                icon=folium.Icon(color='red')
            ).add_to(m)
        else:
            popup_html = ""
            for col in filtered_df.columns:
                if col == "Google Maps Link":
                    popup_html += f'<a href="{row[col]}" target="_blank">View on Google Maps</a><br>'
                else:
                    popup_html += f"<b>{col}:</b> {row[col]}<br>"
            folium.Marker(
                location=[row[lat_col], row[lon_col]],
                popup=popup_html,
                icon=folium.Icon(color='blue')
            ).add_to(m)

            
    # Render the map.
    st_folium(m, width=700, height=500)
else:
    st.error("Latitude and longitude columns not found in the data.")
