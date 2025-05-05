import requests
import sqlite3
import time
import pandas as pd
import os
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import csv
import time

# This script is designed to scrape data from the Indiana Map and the Indiana Nonprofit database,
# and store it in a SQLite database. It also interacts with the Google Solar API to fetch solar data for locations.
class community_solarDatabase:
    def __init__(self):
        self.conn = sqlite3.connect("community_solar.db")
        # you need to create a file called google_api_key.txt and put your google api key in it
        self.api_key = open("google_api_key.txt", "r").read().strip()

    ### Start of locations table methods ###

    def get_locations_data(self):
        # Source page: https://www.indianamap.org/datasets/INMap::address-points-of-indiana-current/explore?location=39.705743%2C-86.396120%2C7.96
        url = "https://hub.arcgis.com/api/download/v1/items/9b222d07cc164eb384a24742cbf1d274/csv?redirect=false&layers=0"
        # fetch the CSV file from the URL
        while True:
            response = requests.get(url)
            data = response.json()
            # check if the status is "Completed"
            if data.get("status") == "Completed":
                result_url = data.get("resultUrl")
                print(
                    "Status is 'Completed'. Proceeding to download CSV from:",
                    result_url,
                )
                break
            # wait for 5 seconds and retry if the status is not "Completed"
            else:
                print(f"Status is '{data.get('status')}'. Waiting and retrying...")
                time.sleep(5)
        # download the CSV file from the result URL
        csv_response = requests.get(result_url)
        if csv_response.ok:
            self.process_locations_data(csv_response)
            print("CSV file downloaded and saved as 'locations_data.csv'.")
        else:
            print("Failed to download CSV. Status code:", csv_response.status_code)

    def process_locations_data(self, csv_response):
        # save the CSV response content to a file
        with open("locations_data.csv", "wb") as file:
            file.write(csv_response.content)
        # read the CSV file into a DataFrame
        df = pd.read_csv("locations_data.csv")
        # drop unnecessary columns
        columns_to_keep = [
            "latitude",
            "longitude",
            "dlgf_prop_class_code",
            "geofulladdress",
            "geocity",
            "geostate",
            "geozip",
            "geocounty",
            "geobg10",
            "geobg20"
        ]
        # modify the dataframe to keep only the necessary columns and save it back to the CSV file
        df = df[columns_to_keep]
        df.to_csv("locations_data.csv", index=False)

    def check_locations_table_exists(self):
        cursor = self.conn.cursor()
        # check if the LOCATIONS table exists in the database
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='LOCATIONS';"
        )
        result = cursor.fetchone()
        if result:
            print("Table 'LOCATIONS' exists.")
        else:
            print("Table 'LOCATIONS' does not exist.")
            self.create_locations_table()
        return result

    def create_locations_table(self):
        cursor = self.conn.cursor()
        # read the CSV file to get the header names (ensure the file name matches)
        df = pd.read_csv("locations_data.csv", low_memory=False)
        header = df.columns
        columns_definitions = []
        columns_definitions.append(f'"location_id" INTEGER PRIMARY KEY AUTOINCREMENT')
        # adding to prevent extra api calls to google solar api
        # 0 indicates we have not checked for data, 
        # 1 indicates we checked and there is no data, 
        # 2 indicates we checked and there is data
        columns_definitions.append(f'"has_solar_data" INTEGER DEFAULT 0')
        column_type_mapping = {
            "latitude": "REAL",
            "longitude": "REAL",
            "dlgf_prop_class_code": "INTEGER"
        }
        # loop through the header names and create the column definitions
        for col in header:
            col_type = column_type_mapping.get(col, "TEXT")
            col_definition = f'"{col}" {col_type}'
            columns_definitions.append(col_definition)
        # create the SQL query to create the table
        columns_sql = ", ".join(columns_definitions)
        table_name = "LOCATIONS"
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_sql});"
        print("Creating table with query:")
        print(create_table_query)
        # create the table in the database
        cursor.execute(create_table_query)
        self.conn.commit()

    def insert_locations_data(self):
        cursor = self.conn.cursor()
        # read the CSV file to get the data to insert into the table
        df = pd.read_csv("locations_data.csv", low_memory=False)
        df.fillna("", inplace=True)
        header = df.columns
        table_name = "LOCATIONS"
        # SQLite uses ? as the placeholder
        insert_query = (
            f"INSERT INTO {table_name} ({', '.join(['\"' + col + '\"' for col in header])}) "
            f"VALUES ({', '.join(['?' for _ in header])})"
        )
        print("Inserting rows:")
        # loop through the rows of the DataFrame and insert them into the table
        for index, row in df.iterrows():
            values = row.tolist()
            try:
                cursor.execute(insert_query, values)
                if index % 1000 == 0:
                    print(f"Inserted row {index}")
            except sqlite3.Error as err:
                print(values)
                print("Error inserting row:", err)
        self.conn.commit()
        print("Data update completed successfully.")

    ### End of locations table methods ###

    ### Start of CEJST table methods ###

    def check_cejst_table_exists(self):
        cursor = self.conn.cursor()
        # check if the CEJST table exists in the database
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='CEJST';"
        )
        result = cursor.fetchone()
        if result:
            print("Table 'CEJST' exists.")
        else:
            print("Table 'CEJST' does not exist.")
            self.create_cejst_table()
        return result
    
    def create_cejst_table(self):
        cursor = self.conn.cursor()
        # create the CEJST table with the specified columns
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS CEJST (
                cejst_id INTEGER PRIMARY KEY AUTOINCREMENT,
                census_tract_2010_ID TEXT,
                identified_as_disadvantaged TEXT,
                date_added DATETIME DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        self.conn.commit()
        
    def insert_cejst_data(self):
        cursor = self.conn.cursor()
        # read the CEJST data from the CSV file
        df = pd.read_csv("cejst_data.csv", low_memory=False)
        # filter the DataFrame to keep only the relevant columns
        df_filtered = df[["Census tract 2010 ID", "Identified as disadvantaged"]].copy()
        df_filtered.rename(
            columns={
                "Census tract 2010 ID": "census_tract_2010_ID",
                "Identified as disadvantaged": "identified_as_disadvantaged",
            },
            inplace=True,
        )
        header = df_filtered.columns
        table_name = "CEJST"
        # SQLite uses ? as the placeholder
        insert_query = (
            f"INSERT INTO {table_name} ({', '.join(['\"' + col + '\"' for col in header])}) "
            f"VALUES ({', '.join(['?' for _ in header])})"
        )
        print("Inserting rows:")
        # loop through the rows of the DataFrame and insert them into the table
        for index, row in df_filtered.iterrows():
            values = row.tolist()
            try:
                cursor.execute(insert_query, values)
                if index % 1000 == 0:
                    print(f"Inserted row {index}")
            except sqlite3.Error as err:
                print(values)
                print("Error inserting row:", err)
        self.conn.commit()

    ### End of CEJST table methods ###

    ### Start of nonprofits table methods ###

    def get_nonprofit_data(self):
        options = Options()
        options.headless = True 
        service = Service(executable_path=ChromeDriverManager().install())
        # set up the Chrome WebDriver
        driver = webdriver.Chrome(service=service, options=options)
        # try to scrape the data from the Indiana Nonprofit database
        try:
            url = "https://www.stats.indiana.edu/nonprofit/inp.aspx"
            driver.get(url)
            # wait for the page to load and the table to be present
            wait = WebDriverWait(driver, 10)
            table = wait.until(EC.presence_of_element_located((By.XPATH, "//table")))
            # wait for the table to load completely
            time.sleep(2)
            # find the table rows and extract the data
            rows = table.find_elements(By.TAG_NAME, "tr")
            data = []
            # loop through the rows and extract the text from each cell
            for row in rows:
                cells = row.find_elements(By.TAG_NAME, "td")
                if not cells:
                    cells = row.find_elements(By.TAG_NAME, "th")
                if cells:
                    data.append([cell.text.strip() for cell in cells])
            # save the data to a CSV file
            with open("nonprofit_data.csv", "w", newline="", encoding="utf-8") as file:
                writer = csv.writer(file)
                writer.writerows(data)
            print("Data scraped successfully and saved to nonprofit_data.csv")
        except Exception as e:
            print("An error occurred:", e)
        finally:
            # close the WebDriver
            driver.quit()

    def check_nonprofits_table_exists(self):
        cursor = self.conn.cursor()
        # check if the NONPROFITS table exists in the database
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='NONPROFITS';"
        )
        result = cursor.fetchone()
        if result:
            print("Table 'NONPROFITS' exists.")
        else:
            print("Table 'NONPROFITS' does not exist.")
            self.create_nonprofits_table()
        return result

    def create_nonprofits_table(self):
        cursor = self.conn.cursor()
        # read the CSV file to get the header names (ensure the file name matches)
        df = pd.read_csv("nonprofit_data.csv", low_memory=False)
        header = df.columns
        columns_definitions = []
        # loop through the header names and create the column definitions
        for col in header:
            col_definition = f'"{col}" TEXT'
            columns_definitions.append(col_definition)
        columns_sql = ", ".join(columns_definitions)
        table_name = "NONPROFITS"
        # create the SQL query to create the table
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_sql});"
        print("Creating table with query:")
        print(create_table_query)
        cursor.execute(create_table_query)
        self.conn.commit()

    def parse_nonprofit_data(self):
        # this function parses the nonprofit data from the downloaded CSV files
        # and combines them into a single DataFrame
        named_download_dir = os.path.abspath("downloads/named/")
        # get the list of files in the directory
        files = os.listdir(named_download_dir)
        dfs = []
        # loop through the files and read each CSV file into a DataFrame
        for file in files:
            county_name = os.path.basename(file).split(".")[0]
            df = pd.read_csv(named_download_dir + "/" + file, skiprows=3)
            df["county"] = county_name
            dfs.append(df)
        # combine the DataFrames into a single DataFrame
        # and save it to a CSV file
        combined_df = pd.concat(dfs, ignore_index=True)
        combined_df.to_csv("nonprofit_data.csv", index=False)

    def insert_nonprofit_data(self):
        cursor = self.conn.cursor()
        # read the CSV file to get the data to insert into the table
        df = pd.read_csv("nonprofit_data.csv", low_memory=False)
        df.fillna("", inplace=True)
        print("Columns:", df.columns.tolist())
        header = df.columns
        table_name = "NONPROFITS"
        # SQLite uses ? as the placeholder
        insert_query = (
            f"INSERT INTO {table_name} ({', '.join(['\"' + col + '\"' for col in header])}) "
            f"VALUES ({', '.join(['?' for _ in header])})"
        )
        print("Inserting rows as a test:")
        # loop through the rows of the DataFrame and insert them into the table
        for index, row in df.iterrows():
            values = row.tolist()
            try:
                cursor.execute(insert_query, values)
                if index % 1000 == 0:
                    print(f"Inserted row {index}")
            except sqlite3.Error as err:
                print("Error inserting row:", err)

        self.conn.commit()
        print("Data update completed successfully.")

    ### End of nonprofits table methods ###

    ### Start of Google Solar API methods ###

    def check_google_solar_table_exists(self):
        cursor = self.conn.cursor()
        # check if the GOOGLE_SOLAR table exists in the database
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='GOOGLE_SOLAR';"
        )
        result = cursor.fetchone()
        if result:
            print("Table 'GOOGLE_SOLAR' exists.")
        else:
            print("Table 'GOOGLE_SOLAR' does not exist.")
            self.create_google_solar_table()
        return result

    def create_google_solar_table(self):
        cursor = self.conn.cursor()
        # create the GOOGLE_SOLAR table with the specified columns
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS GOOGLE_SOLAR (
                solar_id INTEGER PRIMARY KEY AUTOINCREMENT,
                location_id INTEGER NOT NULL,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                imagery_quality TEXT,
                imagery_date DATE,
                max_array_panels_count INTEGER,
                panel_capacity_watts INTEGER,
                nominal_power_watts INTEGER,
                yearly_energy_dc_kwh REAL,
                carbon_offset_factor_kg_per_mwh REAL,
                estimated_annual_co2_savings_tons REAL,
                estimated_houses_powered REAL,
                date_added DATETIME DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        self.conn.commit()

    def get_solar_data(self, latitude, longitude):
        # google solar api has a rate limit of 300 requests per minute
        # if we exceed this limit, we will get a 429 error code
        # and we will need to wait 1 minute before retrying
        time.sleep(.2)  # Sleep for 1 second to avoid hitting the rate limit too quickly
        api_url = f"https://solar.googleapis.com/v1/buildingInsights:findClosest?location.latitude={latitude}&location.longitude={longitude}&requiredQuality=HIGH&key="+self.api_key
        # make the API request
        response = requests.get(api_url)
        solar_data = response.json()
        # check if the request was successful
        if response.status_code != 200:
            print("Response:", solar_data)
            if response.status_code == 429:
                print("Rate limit exceeded. Waiting for 1 minute before retrying...")
                time.sleep(90)
                return self.get_solar_data(latitude, longitude)
        elif response.status_code == 403:
            print("Access denied. Check your API key and permissions.")
            print("Response:", solar_data)
            return response.status_code
        else:
            print("Data fetched successfully")
            # for debugging purposes
            #with open(f"solar_data_{latitude}_{longitude}.json", "w") as f:
                #json.dump(solar_data, f, indent=4)
            return solar_data

    def process_solar_data(self, solar_data):
        # process the solar data and extract the relevant fields
        try:
            # Extract the solar potential data
            solar_potential = solar_data.get("solarPotential", {})
            solar_configs = solar_potential.get("solarPanelConfigs", [])
            best_config = max(solar_configs, key = lambda c: c.get("yearlyEnergyDcKwh", 0)) if solar_configs else {}
            
            # Core fields
            panel_count = solar_potential.get("maxArrayPanelsCount", 0)
            panel_watts = solar_potential.get("panelCapacityWatts", 300)
            carbon_offset_factor = solar_potential.get("carbonOffsetFactorKgPerMwh", 0)
            imagery_quality = solar_data.get("imageryQuality", "UNKNOWN")
            
            # Imagery date
            imagery_date_info = solar_data.get("imageryDate")
            if imagery_date_info:
                try:
                    year = imagery_date_info["year"]
                    month = imagery_date_info["month"]
                    day = imagery_date_info["day"]
                    imagery_date = f"{month:02d}-{day:02d}-{year:04d}"
                except (KeyError, TypeError, ValueError):
                    imagery_date = None
            else:
                imagery_date = None
            
            # Derived metrics
            nominal_power_watts = panel_count * panel_watts
            yearly_energy_kwh = best_config.get("yearlyEnergyDcKwh")
            
            # Fallback: estimate energy using whole sun quant (if available)
            if not yearly_energy_kwh:
                whole_sun_quant = solar_potential.get("maxSunshineHoursPerYear", 1000)
                estimated_kwh = (nominal_power_watts / 1000) * whole_sun_quant * 0.8
                yearly_energy_kwh = round(estimated_kwh, 2)
            
            # CO2 savings in tons based on EPA estimate
            co2_savings_tons = yearly_energy_kwh * 0.000699
            
            # Houses powered (based on US average usage)
            houses_powered = round(yearly_energy_kwh / 10566, 2)
            
            return {
                "imageryQuality": imagery_quality,
                "imageryDate": imagery_date,
                "maxArrayPanelsCount": panel_count,
                "panelCapacityWatts": panel_watts,
                "nominalPowerWatts": nominal_power_watts, # Manual
                "yearlyEnergyDcKwh": yearly_energy_kwh,
                "carbonOffsetFactorKgPerMwh": carbon_offset_factor,
                "estimatedAnnualCO2SavingsTons": round(co2_savings_tons, 2), # Manual
                "estimatedHousesPowered": houses_powered # Manual
            }
        
        except KeyError:
            print(f"Missing field in response: {e}")
            return None

    def get_and_insert_solar_data(self, limit=5):
        cursor = self.conn.cursor()
        # get the locations that need solar data
        # normally run this with a limit of 5 to test the code
        # but for the final run, you can up the limit to 1000 or more but be careful of the rate limit and computer limits
        cursor.execute(f"""
            SELECT 
                location_id, latitude, longitude, has_solar_data
            FROM 
                LOCATIONS 
            WHERE 
                location_id NOT IN (SELECT location_id FROM GOOGLE_SOLAR) 
            AND 
                has_solar_data = 1
            AND 
                dlgf_prop_class_code LIKE '6%%'
            ORDER BY 
                location_id 
            LIMIT 
                ?
            """, (limit,))
        # get the locations that need solar data
        locations = cursor.fetchall()
        for location in locations:
            location_id, latitude, longitude, has_solar_data = location
            print(f"Processing location {location_id} with latitude {latitude} and longitude {longitude}")
            solar_data = self.get_solar_data(latitude, longitude)
            # check if the solar data is valid
            if solar_data:
                # process the solar data and insert it into the database
                processed_data = self.process_solar_data(solar_data)
                if processed_data:
                    print("Processed data for location: ", location_id)
                    # update the has_solar_data field in the LOCATIONS table
                    cursor.execute(
                        """
                        UPDATE LOCATIONS 
                        SET has_solar_data = 2
                        WHERE location_id = ?
                        """,
                        (location_id,)
                    )
                    # insert the solar data into the GOOGLE_SOLAR table 
                    cursor.execute(
                        """
                        INSERT INTO GOOGLE_SOLAR (
                            location_id, 
                            latitude, 
                            longitude, 
                            imagery_quality, 
                            imagery_date, 
                            max_array_panels_count,
                            panel_capacity_watts, 
                            nominal_power_watts, 
                            yearly_energy_dc_kwh, 
                            carbon_offset_factor_kg_per_mwh, 
                            estimated_annual_co2_savings_tons, 
                            estimated_houses_powered
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            location_id,
                            latitude,
                            longitude,
                            processed_data["imageryQuality"],
                            processed_data["imageryDate"],
                            processed_data["maxArrayPanelsCount"],
                            processed_data["panelCapacityWatts"],
                            processed_data["nominalPowerWatts"],
                            processed_data["yearlyEnergyDcKwh"],
                            processed_data["carbonOffsetFactorKgPerMwh"],
                            processed_data["estimatedAnnualCO2SavingsTons"],
                            processed_data["estimatedHousesPowered"]
                        )
                    )
                    self.conn.commit()
            elif solar_data == 403:
                return
            else:
                # if the solar data is not valid, update the has_solar_data field in the LOCATIONS table
                print(f"No solar data found for location {location_id}.")
                cursor.execute(
                    """
                    UPDATE LOCATIONS 
                    SET has_solar_data = 1 
                    WHERE location_id = ?
                    """,
                    (location_id,)
                )
                self.conn.commit()
        self.conn.commit()
        self.conn.close()

    ### End of Google Solar API methods ###

    ### Start of property codes table methods ###
    
    def check_property_codes_table_exists(self):
        cursor = self.conn.cursor()
        # check if the PROPERTY_CODES table exists in the database
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='PROPERTY_CODES';"
        )
        result = cursor.fetchone()
        if result:
            print("Table 'PROPERTY_CODES' exists.")
        else:
            print("Table 'PROPERTY_CODES' does not exist.")
            self.create_property_codes_table()
    
    def create_property_codes_table(self):
        cursor = self.conn.cursor()
        # create the PROPERTY_CODES table with the specified columns
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS PROPERTY_CODES (
                property_code_id INTEGER PRIMARY KEY AUTOINCREMENT,
                property_code TEXT,
                description TEXT,
                name TEXT,
                date_added DATETIME DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        self.conn.commit()

    def insert_property_codes_data(self):
        cursor = self.conn.cursor()
        # read the CSV file to get the data to insert into the table
        df = pd.read_csv("property_codes.csv", low_memory=False)
        df.fillna("", inplace=True)
        print("Columns:", df.columns.tolist())
        header = df.columns
        table_name = "PROPERTY_CODES"
        # SQLite uses ? as the placeholder
        insert_query = (
            f"INSERT INTO {table_name} ({', '.join(['\"' + col + '\"' for col in header])}) "
            f"VALUES ({', '.join(['?' for _ in header])})"
        )
        print("Inserting rows:")
        # loop through the rows of the DataFrame and insert them into the table
        for index, row in df.iterrows():
            values = row.tolist()
            try:
                cursor.execute(insert_query, values)
                if index % 1000 == 0:
                    print(f"Inserted row {index}")
            except sqlite3.Error as err:
                print(values)
                print("Error inserting row:", err)
        self.conn.commit()

    ### End of property codes table methods ###

    def clear_database(self):
        # This function clears the database by dropping all tables
        # and resetting the database to its initial state.
        cursor = self.conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS GOOGLE_SOLAR;")
        cursor.execute("DROP TABLE IF EXISTS LOCATIONS;")
        cursor.execute("DROP TABLE IF EXISTS NONPROFITS;")
        cursor.execute("DROP TABLE IF EXISTS CEJST;")
        cursor.execute("DROP TABLE IF EXISTS PROPERTY_CODES;")
        self.conn.commit()
        print("Database cleared.")

    def export_data_dictionary_to_excel(self):
        cursor = self.conn.cursor()
        # column definitions for the tables
        column_definitions = {
        "location_id": "The unique identifier for the location.",
        "has_solar_data": "Indicates if solar data has been fetched for the location. 0: No data, 1: No solar data, 2: Solar data available.",
        "latitude": "The latitude of the location.",
        "longitude": "The longitude of the location.",
        "dlgf_prop_class_code": "The property class code from the Indiana Department of Local Government Finance.",
        "geofulladdress": "The full address of the location.",
        "geocity": "The city of the location.",
        "geostate": "The state of the location.",
        "geozip": "The ZIP code of the location.",
        "geocounty": "The county of the location.",
        "geobg10": "The 2010 census block group of the location.",
        "geobg20": "The 2020 census block group of the location.",
        "solar_id": "The unique identifier for the solar data.",
        "location_id": "The unique identifier for the location associated with the solar data.",
        "latitude": "The latitude of the location.",
        "longitude": "The longitude of the location.",
        "imagery_quality": "The quality of the imagery used for the solar data.",
        "imagery_date": "The date of the imagery used for the solar data.",
        "max_array_panels_count": "The maximum number of solar panels in the array.",
        "panel_capacity_watts": "The capacity of each solar panel in watts.",
        "nominal_power_watts": "The nominal power of the solar array in watts.",
        "yearly_energy_dc_kwh": "The estimated yearly energy output of the solar array in kilowatt-hours.",
        "carbon_offset_factor_kg_per_mwh": "The carbon offset factor in kilograms per megawatt-hour.",
        "estimated_annual_co2_savings_tons": "The estimated annual CO2 savings in tons.",
        "estimated_houses_powered": "The estimated number of houses powered by the solar array.",
        "date_added": "The date the solar data was added to the database.",
        "cejst_id": "The unique identifier for the CEJST data.",
        "census_tract_2010_ID": "The 2010 census tract ID.",
        "identified_as_disadvantaged": "Indicates if the census tract is identified as disadvantaged.",
        "date_added": "The date the CEJST data was added to the database.",
        "property_code_id": "The unique identifier for the property code.",
        "property_code": "The property code.",
        "description": "The description of the property code.",
        "name": "The name of the property code.",
        "date_added": "The date the property code was added to the database."
        }
        # retrieve all table names
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [table[0] for table in cursor.fetchall()]
        # create an Excel writer object using pandas
        writer = pd.ExcelWriter("community_solar_database_data_dictionary.xlsx", engine='xlsxwriter')
        for table_name in tables:
            # get the structure of the table using PRAGMA table_info
            cursor.execute(f"PRAGMA table_info({table_name});")
            columns = cursor.fetchall()
            # add the column descriptions to the columns list
            columns = [(col[0], col[1], col[2], col[3], col[4], col[5], column_definitions.get(col[1], "")) for col in columns]
            # each entry in columns is a tuple:
            # (cid, column_name, data_type, notnull, default_value, pk)
            df = pd.DataFrame(columns, columns=['cid', 'column_name', 'data_type', 'notnull', 'default_value', 'pk', 'column_definition'])
            # write the DataFrame to a separate sheet named after the table
            df.to_excel(writer, sheet_name=table_name, index=False)
        # save the Excel file and close the writer
        writer.close()

    def create_database_and_build(self):
        # Gets the newest address data from the Indiana map and saves it to a CSV file
        # Creates the LOCATIONS table and inserts the address data into the table for every location
        self.get_locations_data()
        self.check_locations_table_exists()
        self.insert_locations_data()

        '''
        We have currently dropped this table to save time and space.
        The data is not needed for the current project, but it can be added back in if needed.
        '''
        # Creates the NONPROFITS table and inserts the nonprofit data into the table for every location
        #self.get_nonprofit_data() # Only run this if need be, will take awhile to scrape the data
        #self.check_nonprofits_table_exists()
        #self.parse_nonprofit_data()
        #self.insert_nonprofit_data()

        # Creates the GOOGLE_SOLAR table
        self.check_google_solar_table_exists()

        # Creates the CEJST table and inserts the CEJST data into the table for every location
        # You must download the CEJST data from the CEJST website and save it to a CSV file
        # https://edgi-govdata-archiving.github.io/j40-cejst-2/en/downloads#7.06/40.074/-86.111
        self.check_cejst_table_exists()
        self.insert_cejst_data()

        # Creates the PROPERTY_CODES table and inserts the property codes data into the table
        self.check_property_codes_table_exists()
        self.insert_property_codes_data()

        # Export the database structure to an Excel file
        self.export_data_dictionary_to_excel()

        self.conn.close()

if __name__ == "__main__":

    db = community_solarDatabase()

    # create the database and build the tables
    #db.create_database_and_build()

    # export the database structure to an Excel file
    #db.export_data_dictionary_to_excel()

    # Get and update solar data for the locations in the database
    #db.get_and_insert_solar_data(limit=20)


