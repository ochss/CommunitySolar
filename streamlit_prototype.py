import streamlit as st
import sqlite3
import pandas as pd
import folium
from streamlit_folium import st_folium

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
    
    # Convert the property code field to string to ensure sorting/filtering works correctly.
    df['dlgf_prop_class_code'] = df['dlgf_prop_class_code'].astype(str)

    df['Google Maps Link'] = df.apply(
        lambda row: f'<a href="https://www.google.com/maps/search/?api=1&query={row["latitude"]},{row["longitude"]}" target="_blank">View on Google Maps</a>', axis=1
    )
    
    # Convert production values to numeric (if not already) to facilitate filtering.
    df['yearly_energy_production'] = pd.to_numeric(df['yearly_energy_production'], errors='coerce')
    return df

# Load the data from the database.
df = load_data()
#st.set_page_config(layout="wide") 
st.title("Community Solar Locations in Valparaiso")
st.subheader("Data Preview")
st.dataframe(df.head())

## Filter 1: Select Property Code ##

# Create a dropdown for unique dlgf_prop_class_code values.
codes = sorted(df['dlgf_prop_class_code'].unique())
selected_code = st.selectbox("Select a Property Code", codes)

# Filter the DataFrame based on the selected property code.
filtered_df = df[df['dlgf_prop_class_code'] == selected_code]

## Filter 2: Yearly Energy Production via Quartiles ##
if not filtered_df.empty:
    # Drop rows missing production values.
    filtered_df = filtered_df.dropna(subset=['yearly_energy_production'])
    
    # Calculate quartiles for the 'yearly_energy_production' field.
    quartiles = filtered_df['yearly_energy_production'].quantile([0.25, 0.50, 0.75]).to_dict()
    
    # Build a dictionary of options with the minimum threshold values.
    quartile_options = {
        "All": filtered_df['yearly_energy_production'].min(),
        f"Q1 (>= {quartiles[0.25]:.2f})": quartiles[0.25],
        f"Q2 (>= {quartiles[0.50]:.2f})": quartiles[0.50],
        f"Q3 (>= {quartiles[0.75]:.2f})": quartiles[0.75]
    }
    
    selected_quartile = st.selectbox("Minimum Yearly Energy Production", list(quartile_options.keys()))
    min_yearly_energy = quartile_options[selected_quartile]
    
    # Apply the yearly energy production filter.
    filtered_df = filtered_df[filtered_df['yearly_energy_production'] >= min_yearly_energy]
else:
    st.warning("No data available for the selected property code.")

## Mapping ##
# Define the expected latitude and longitude column names.
lat_col = 'latitude'
lon_col = 'longitude'

if lat_col in filtered_df.columns and lon_col in filtered_df.columns:
    # Remove rows with missing coordinate values and ensure they are floats.
    filtered_df = filtered_df.dropna(subset=[lat_col, lon_col])
    filtered_df[lat_col] = filtered_df[lat_col].astype(float)
    filtered_df[lon_col] = filtered_df[lon_col].astype(float)
    
    # Create a Folium map centered around the average location.
    center = [filtered_df[lat_col].mean(), filtered_df[lon_col].mean()]
    m = folium.Map(location=center, zoom_start=12)
    
    # Add a marker for each location. Each marker’s popup shows all of its details.
    for index, row in filtered_df.iterrows():
        popup_html = ""
        for col in filtered_df.columns:
            popup_html += f"<b>{col}:</b> {row[col]}<br>"
        folium.Marker(
            location=[row[lat_col], row[lon_col]],
            popup=popup_html
        ).add_to(m)
    
    # Render the Folium map within Streamlit and capture click events.
    map_response = st_folium(m, width=1200, height=1000)
    
    # Check for click events. st_folium returns a dictionary with key "last_clicked".
    if map_response and "last_clicked" in map_response and map_response["last_clicked"]:
        clicked_coords = map_response["last_clicked"]
        clicked_lat = clicked_coords["lat"]
        clicked_lon = clicked_coords["lng"]
        
        # Compute Euclidean distance to find the nearest marker.
        # (This approximation works well for small geographic areas.)
        filtered_df = filtered_df.copy()  # Avoid modifying the original DataFrame.
        filtered_df["distance"] = ((filtered_df[lat_col] - clicked_lat)**2 + (filtered_df[lon_col] - clicked_lon)**2)**0.5
        
        # Select the nearest marker.
        nearest = filtered_df.nsmallest(1, "distance")
        threshold = 0.001  # Adjust threshold (in degrees) as needed.
        if nearest["distance"].iloc[0] < threshold:
            st.markdown("### Details of Selected Location")
            st.write(nearest.drop(columns=["distance"]).T)
        else:
            st.info("Click closer to a marker to see its details.")
else:
    st.error("Latitude and longitude columns not found in the data.")
