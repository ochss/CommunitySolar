import streamlit as st
import sqlite3
import pandas as pd
import folium
from streamlit_folium import st_folium
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode

# set the page config for a wide layout and title
st.set_page_config(layout="wide", page_title="Community Solar Locations in Indiana")

# Data loading function with caching for performance.
@st.cache_data
def load_data():
    # Connect to the SQLite database and load the data into a DataFrame.
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
        GOOGLE_SOLAR.estimated_houses_powered,
        PROPERTY_CODES.description AS property_code_description,
        CASE CEJST.identified_as_disadvantaged
            WHEN 1 THEN 'yes'
            ELSE 'no'
        END AS identified_as_disadvantaged
    FROM 
        LOCATIONS
    INNER JOIN 
        GOOGLE_SOLAR
    ON
        GOOGLE_SOLAR.location_id = LOCATIONS.location_id 
    INNER JOIN 
        PROPERTY_CODES
    ON
        LOCATIONS.dlgf_prop_class_code = PROPERTY_CODES.property_code
    LEFT JOIN
        CEJST
    ON
        SUBSTR(LOCATIONS.geobg10, 1, 11) = CEJST.census_tract_2010_ID
    WHERE 
        LOCATIONS.dlgf_prop_class_code LIKE '6%%'
    """
    df = pd.read_sql_query(query, conn)
    conn.close()

    # add a new column for the age of solar imagery in years
    df['age_of_solar_imagery(years)'] = pd.to_datetime('now').year - pd.to_datetime(df['imagery_date']).dt.year

    # convert the imagery quality to a string and replace spaces with underscores
    df['imagery_quality'] = df['imagery_quality'].astype(str).str.replace(' ', '_').str.lower()
    
    # convert the property code to numeric, coercing errors to NaN
    df['dlgf_prop_class_code'] = pd.to_numeric(df['dlgf_prop_class_code'], errors='coerce')

    # create a google maps link for each location
    df['Google Maps Link'] = df.apply(lambda row: f'https://www.google.com/maps/search/?api=1&query={row["latitude"]},{row["longitude"]}', axis=1)
    # ensure it's a string for HTML rendering.
    df['Google Maps Link'] = df['Google Maps Link'].astype(str)  

    # columns to drop
    columns_to_drop = ['imagery_quality', 'imagery_date', 'location_id']
    # drop the columns
    df.drop(columns=columns_to_drop, inplace=True, errors='ignore')

    # rename columns for better readability
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
        'estimated_houses_powered': 'Estimated Houses Powered',
        'property_code_description': 'Property Code Description',
        'identified_as_disadvantaged': 'Disadvantaged Flag',
        'latitude': 'Latitude',
        'longitude': 'Longitude'
    }, inplace=True)

    # reorder columns for better readability
    column_order = [
        'Full Address', 'City', 'Zip Code', 'Property Code', 'Disadvantaged Flag',
        'Max Array Panels Count', 'Panel Capacity (Watts)', 'Nominal Power (Watts)',
        'Yearly Energy DC (kWh)', 'Carbon Offset Factor (kg/MWh)',
        'Estimated Annual CO2 Savings (tons)', 'Estimated Houses Powered',
        'Age of Solar Imagery (years)', 'Property Code Description', 'Latitude', 'Longitude', 'Google Maps Link'
    ]
    # reorder the df
    df = df[column_order]

    return df

# Load the data.
df = load_data()


#st.title("Community Solar Locations in Indiana")

# create three columns for the filters.
col1, col2, col3 = st.columns(3)

with col1:
    cities = sorted(df['City'].unique())
    # no default selection
    selected_cities = st.multiselect("Select City(s)", cities)

with col2:
    # get unique property codes and descriptions combine into a single string
    property_code_descriptions = (df['Property Code'].astype(str) + " - " + df['Property Code Description'].astype(str))
    codes = sorted(property_code_descriptions.unique())
    # no default selection
    selected_codes = st.multiselect("Select Property Code(s)", codes)

with col3:
    disadvantaged = sorted(df['Disadvantaged Flag'].unique())
    # no default selection
    selected_disadvantaged = st.multiselect("Select Disadvantaged Flag", disadvantaged)

# only filter and render data if all filters have a selection.
if selected_cities and selected_codes and selected_disadvantaged:
    # remove the description part from the selected codes for filtering.
    selected_codes = [int(code.split("-")[0]) for code in selected_codes]
    print(selected_codes)
    # filter the dataframe based on the selected filters.
    filtered_df = df[df['City'].isin(selected_cities) & df['Property Code'].isin(selected_codes) & df['Disadvantaged Flag'].isin(selected_disadvantaged)]
    # error handling for empty dataframe.
    if filtered_df.empty:
        st.warning("⚠️ No data found for the selected filter combination. Please try different selections.")
    else:
        # convert filtered dataframe to CSV.
        csv = filtered_df.to_csv(index=False).encode('utf-8')
        # create a download button for the CSV file.
        st.download_button(
            label="Export filtered data as CSV",
            data=csv,
            file_name='filtered_solar_locations.csv',
            mime='text/csv'
        )

        # interactive Data Preview using AgGrid 
        st.subheader("Data: (Click a row to show it on the map)")
        gb = GridOptionsBuilder.from_dataframe(filtered_df)

        # configure AgGrid to allow single row selection.
        gb.configure_selection(selection_mode="single", use_checkbox=False)
        gb.configure_pagination(paginationAutoPageSize=True)  # Enable pagination
        
        # configure columns to be sortable and filterable.
        gridOptions = gb.build()

        # display the grid and capture the response.
        grid_response = AgGrid(
            filtered_df,
            gridOptions=gridOptions,
            update_mode=GridUpdateMode.SELECTION_CHANGED,
            theme='blue'
        )

        # check the selected row.
        selected_rows = grid_response.get('selected_rows')
        selected_location = None

        # check if selected_rows is a DataFrame and if it's not empty.
        if isinstance(selected_rows, pd.DataFrame):
            if not selected_rows.empty:
                selected_location = selected_rows.iloc[0]  # Use .iloc[0] to get the first row as a Series.
                st.markdown("### Selected Location Details")
                st.write(selected_location)
        else:
            # fallback in case it is not a DataFrame.
            if selected_rows and len(selected_rows) > 0:
                selected_location = selected_rows[0]
                st.markdown("### Selected Location Details")
                st.write(selected_location)

        # latitude and longitude columns.
        lat_col = 'Latitude'
        lon_col = 'Longitude'

        if lat_col in filtered_df.columns and lon_col in filtered_df.columns:
            # remove rows with missing coordinates and convert to float.
            filtered_df = filtered_df.dropna(subset=[lat_col, lon_col])
            filtered_df[lat_col] = filtered_df[lat_col].astype(float)
            filtered_df[lon_col] = filtered_df[lon_col].astype(float)

            # set default map center to the average of available points.
            default_center = [filtered_df[lat_col].mean(), filtered_df[lon_col].mean()]

            # if a row was selected, update the map center.
            if selected_location is not None:
                default_center = [selected_location[lat_col], selected_location[lon_col]]
            
            # create a folium map centered at the default location.
            m = folium.Map(location=default_center, zoom_start=12)
            
            # add markers to the map.
            for idx, row in filtered_df.iterrows():
                # use a different marker color for the selected row.
                if selected_location is not None and row[lat_col] == selected_location[lat_col] and row[lon_col] == selected_location[lon_col]:
                    popup_html = "<b>Selected Location</b><br>"
                    for col in filtered_df.columns:
                        if col == "Google Maps Link":
                            popup_html += f'<a href="{row[col]}" target="_blank">View on Google Maps</a><br>'
                        else:
                            popup_html += f"<b>{col}:</b> {row[col]}<br>"
                    folium.Marker(
                        location=[row[lat_col], row[lon_col]],
                        popup = folium.Popup(popup_html, min_width=300, max_width=500),
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
                        popup=folium.Popup(popup_html, min_width=300, max_width=500),
                        icon=folium.Icon(color='blue')
                    ).add_to(m)

                    
            # render the map.
            st_folium(m, width=1200, height=1000)
else:
    st.info("Please select all filters to display data.")
