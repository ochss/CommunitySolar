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
        LOCATIONS.geofulladdress,
        LOCATIONS.geocity,
        LOCATIONS.geozip,
        GOOGLE_SOLAR.imagery_quality,
        GOOGLE_SOLAR.imagery_date,
        GOOGLE_SOLAR.max_array_panels_count,
        GOOGLE_SOLAR.panel_capacity_watts,
        GOOGLE_SOLAR.nominal_power_watts,
        GOOGLE_SOLAR.yearly_energy_dc_kwh,
        GOOGLE_SOLAR.carbon_offset_factor_kg_per_mwh,
        GOOGLE_SOLAR.estimated_annual_co2_savings_tons,
        GOOGLE_SOLAR.estimated_houses_powered

    FROM 
        LOCATIONS
    INNER JOIN 
        GOOGLE_SOLAR
    ON
        GOOGLE_SOLAR.location_id = LOCATIONS.location_id 
    WHERE 
        LOCATIONS.dlgf_prop_class_code LIKE '6%%'
    """
    df = pd.read_sql_query(query, conn)
    conn.close()

    df['age_of_solar_imagery(years)'] = pd.to_datetime('now').year - pd.to_datetime(df['imagery_date']).dt.year
    df['imagery_quality'] = df['imagery_quality'].astype(str).str.replace(' ', '_').str.lower()
    
    df['dlgf_prop_class_code'] = pd.to_numeric(df['dlgf_prop_class_code'], errors='coerce')

    df['Google Maps Link'] = df.apply(
        #lambda row: f'<a href="https://www.google.com/maps/search/?api=1&query={row["latitude"]},{row["longitude"]}" target="_blank">View on Google Maps</a>', 
        lambda row: f'https://www.google.com/maps/search/?api=1&query={row["latitude"]},{row["longitude"]}', 
        axis=1
    )
    df['Google Maps Link'] = df['Google Maps Link'].astype(str)  # Ensure it's a string for HTML rendering.
    # Columns to drop
    columns_to_drop = ['imagery_quality', 'imagery_date', 'location_id']
    # Drop the columns
    df.drop(columns=columns_to_drop, inplace=True, errors='ignore')
    print(df.columns)
    # Rename columns for better readability
    df.rename(columns={
        'geofulladdress': 'Full Address',
        'geocity': 'City',
        'geozip': 'Zip Code',
        'dlgf_prop_class_code': 'Property Code',
        'age_of_solar_imagery(years)': 'Age of Solar Imagery (years)',
        'Google Maps Link': 'Google Maps Link',
        'max_array_panels_count': 'Max Array Panels Count',
        'panel_capacity_watts': 'Panel Capacity (Watts)',
        'nominal_power_watts': 'Nominal Power (Watts)',
        'yearly_energy_dc_kwh': 'Yearly Energy DC (kWh)',
        'carbon_offset_factor_kg_per_mwh': 'Carbon Offset Factor (kg/MWh)',
        'estimated_annual_co2_savings_tons': 'Estimated Annual CO2 Savings (tons)',
        'estimated_houses_powered': 'Estimated Houses Powered'
    }, inplace=True)
    # Reorder columns for better readability
    column_order = [
        'Full Address', 'City', 'Zip Code', 'Property Code',
        'Max Array Panels Count', 'Panel Capacity (Watts)', 'Nominal Power (Watts)',
        'Yearly Energy DC (kWh)', 'Carbon Offset Factor (kg/MWh)',
        'Estimated Annual CO2 Savings (tons)', 'Estimated Houses Powered',
        'Age of Solar Imagery (years)', 'Google Maps Link', 'latitude', 'longitude'
    ]
    df = df[column_order]

    return df

# Load the data.
df = load_data()

st.title("Community Solar Locations in Indiana")

# Create two columns for the filters.
col1, col2 = st.columns(2)

with col1:
    cities = sorted(df['City'].unique())
    # No default selection
    selected_cities = st.multiselect("Select City(s)", cities)

with col2:
    codes = sorted(df['Property Code'].unique())
    # No default selection
    selected_codes = st.multiselect("Select Property Code(s)", codes)

# Only filter and render data if both filters have a selection.
if selected_cities and selected_codes:
    # Filter the dataframe based on the selected cities and property codes.
    filtered_df = df[df['City'].isin(selected_cities) & df['Property Code'].isin(selected_codes)]

    # Convert filtered dataframe to CSV.
    csv = filtered_df.to_csv(index=False).encode('utf-8')

    st.download_button(
        label="Export filtered data as CSV",
        data=csv,
        file_name='filtered_solar_locations.csv',
        mime='text/csv'
    )

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
    st.info("Please select one or more filters to display data.")
