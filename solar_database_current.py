import requests
import sqlite3
import time
import pandas as pd
import os
import json

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import csv
import time


class community_solarDatabase:
    def __init__(self):
        self.conn = sqlite3.connect("community_solar.db")
        self.api_key = open("google_api_key.txt", "r").read().strip()

    ### Start of locations table methods ###

    def get_locations_data(self):
        # Source page: https://www.indianamap.org/datasets/INMap::address-points-of-indiana-current/explore?location=39.705743%2C-86.396120%2C7.96
        url = "https://hub.arcgis.com/api/download/v1/items/9b222d07cc164eb384a24742cbf1d274/csv?redirect=false&layers=0"

        while True:
            response = requests.get(url)
            data = response.json()
            if data.get("status") == "Completed":
                result_url = data.get("resultUrl")
                print(
                    "Status is 'Completed'. Proceeding to download CSV from:",
                    result_url,
                )
                break
            else:
                print(f"Status is '{data.get('status')}'. Waiting and retrying...")
                time.sleep(5)

        csv_response = requests.get(result_url)
        if csv_response.ok:
            self.process_locations_data(csv_response)
            print("CSV file downloaded and saved as 'locations_data.csv'.")
        else:
            print("Failed to download CSV. Status code:", csv_response.status_code)

    def process_locations_data(self, csv_response):
        with open("locations_data.csv", "wb") as file:
            file.write(csv_response.content)
        df = pd.read_csv("locations_data.csv")
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
        df = df[columns_to_keep]
        df.to_csv("locations_data.csv", index=False)

    def check_locations_table_exists(self):
        cursor = self.conn.cursor()
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
        # Read the CSV file to get the header names (ensure the file name matches)
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
        for col in header:
            col_type = column_type_mapping.get(col, "TEXT")
            col_definition = f'"{col}" {col_type}'
            columns_definitions.append(col_definition)
        columns_sql = ", ".join(columns_definitions)
        table_name = "LOCATIONS"
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_sql});"
        print("Creating table with query:")
        print(create_table_query)
        cursor.execute(create_table_query)
        self.conn.commit()

    def insert_locations_data(self):
        cursor = self.conn.cursor()
        df = pd.read_csv("locations_data.csv", low_memory=False)
        df.fillna("", inplace=True)
        print("Columns:", df.columns.tolist())
        header = df.columns
        table_name = "LOCATIONS"
        # SQLite uses ? as the placeholder
        insert_query = (
            f"INSERT INTO {table_name} ({', '.join(['\"' + col + '\"' for col in header])}) "
            f"VALUES ({', '.join(['?' for _ in header])})"
        )
        print("Inserting rows:")
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
        df = pd.read_csv("cejst_data.csv", low_memory=False)
        df_filtered = df[["Census tract 2010 ID", "Identified as disadvantaged"]].copy()
        df_filtered.rename(
            columns={
                "Census tract 2010 ID": "census_tract_2010_ID",
                "Identified as disadvantaged": "identified_as_disadvantaged",
            },
            inplace=True,
        )
        print("Columns:", df_filtered.columns.tolist())
        header = df_filtered.columns
        table_name = "CEJST"
        # SQLite uses ? as the placeholder
        insert_query = (
            f"INSERT INTO {table_name} ({', '.join(['\"' + col + '\"' for col in header])}) "
            f"VALUES ({', '.join(['?' for _ in header])})"
        )
        print("Inserting rows:")
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

        driver = webdriver.Chrome(service=service, options=options)

        try:
            url = "https://www.stats.indiana.edu/nonprofit/inp.aspx"
            driver.get(url)

            wait = WebDriverWait(driver, 10)
            table = wait.until(EC.presence_of_element_located((By.XPATH, "//table")))

            time.sleep(2)

            rows = table.find_elements(By.TAG_NAME, "tr")
            data = []
            
            for row in rows:
                cells = row.find_elements(By.TAG_NAME, "td")
                if not cells:
                    cells = row.find_elements(By.TAG_NAME, "th")
                if cells:
                    data.append([cell.text.strip() for cell in cells])

            with open("nonprofit_data.csv", "w", newline="", encoding="utf-8") as file:
                writer = csv.writer(file)
                writer.writerows(data)

            print("Data scraped successfully and saved to nonprofit_data.csv")

        except Exception as e:
            print("An error occurred:", e)
        finally:
            driver.quit()

    def check_nonprofits_table_exists(self):
        cursor = self.conn.cursor()
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
        df = pd.read_csv("nonprofit_data.csv", low_memory=False)
        header = df.columns
        columns_definitions = []
        for col in header:
            col_definition = f'"{col}" TEXT'
            columns_definitions.append(col_definition)
        columns_sql = ", ".join(columns_definitions)
        table_name = "NONPROFITS"
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_sql});"
        print("Creating table with query:")
        print(create_table_query)
        cursor.execute(create_table_query)
        self.conn.commit()

    def parse_nonprofit_data(self):
        named_download_dir = os.path.abspath("downloads/named/")
        files = os.listdir(named_download_dir)
        dfs = []
        for file in files:
            county_name = os.path.basename(file).split(".")[0]
            df = pd.read_csv(named_download_dir + "/" + file, skiprows=3)
            df["county"] = county_name
            dfs.append(df)
        combined_df = pd.concat(dfs, ignore_index=True)
        combined_df.to_csv("nonprofit_data.csv", index=False)

    def insert_nonprofit_data(self):
        cursor = self.conn.cursor()
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
        response = requests.get(api_url)
        solar_data = response.json()
        if response.status_code != 200:
            #print(f"Error fetching data: {response.status_code}")
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
        try:
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

    def get_and_insert_solar_data(self):
        cursor = self.conn.cursor()
        # When not in google solar table, get the first 10 locations from the LOCATIONS table
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
                5
            """)
        locations = cursor.fetchall()
        for location in locations:
            location_id, latitude, longitude, has_solar_data = location
            print(f"Processing location {location_id} with latitude {latitude} and longitude {longitude}")
            solar_data = self.get_solar_data(latitude, longitude)
            if solar_data:
                processed_data = self.process_solar_data(solar_data)
                if processed_data:
                    print("Processed data for location: ", location_id)
                    cursor.execute(
                        """
                        UPDATE LOCATIONS 
                        SET has_solar_data = 2
                        WHERE location_id = ?
                        """,
                        (location_id,)
                    )
                    self.conn.commit()
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
        cursor.execute("DROP TABLE IF EXISTS PROPERTY_CODES;")
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
        cursor = self.conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS GOOGLE_SOLAR;")
        cursor.execute("DROP TABLE IF EXISTS LOCATIONS;")
        cursor.execute("DROP TABLE IF EXISTS NONPROFITS;")
        cursor.execute("DROP TABLE IF EXISTS CEJST;")
        self.conn.commit()
        print("Database cleared.")

    def show_db_structure(self):
        cursor = self.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        if not tables:
            print("No tables found in the database.")
            return
        for table in tables:
            table_name = table[0]
            print(f"Table: {table_name}")
            cursor.execute(f"PRAGMA table_info({table_name});")
            columns = cursor.fetchall()
            if columns:
                print(" Columns:")
                for col in columns:
                    cid, name, col_type, notnull, dflt_value, pk = col
                    print(f"  - {name} ({col_type}){' PRIMARY KEY' if pk else ''}{' NOT NULL' if notnull else ''}")
            else:
                print(" No columns found.")
            print() 

    def export_db_structure_to_excel(self):
        cursor = self.conn.cursor()
        
        # Retrieve all table names
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [table[0] for table in cursor.fetchall()]
        
        # Create an Excel writer object using pandas
        writer = pd.ExcelWriter("db_structure.xlsx", engine='xlsxwriter')
        
        for table_name in tables:
            # Get the structure of the table using PRAGMA table_info
            cursor.execute(f"PRAGMA table_info({table_name});")
            columns = cursor.fetchall()
            
            # Each entry in columns is a tuple:
            # (cid, column_name, data_type, notnull, default_value, pk)
            df = pd.DataFrame(columns, columns=['cid', 'column_name', 'data_type', 'notnull', 'default_value', 'pk'])
            
            # Write the DataFrame to a separate sheet named after the table
            df.to_excel(writer, sheet_name=table_name, index=False)
        
        # Save the Excel file and close the writer
        writer.close()

    def create_database_and_build(self):
        # Gets the newest address data from the Indiana map and saves it to a CSV file
        # Creates the LOCATIONS table and inserts the address data into the table for every location
        #self.get_locations_data()
        #self.check_locations_table_exists()
        #self.insert_locations_data()

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
        #self.check_google_solar_table_exists()

        # Creates the CEJST table and inserts the CEJST data into the table for every location
        #self.check_cejst_table_exists()
        #self.insert_cejst_data()

        # Creates the PROPERTY_CODES table and inserts the property codes data into the table
        self.check_property_codes_table_exists()
        self.insert_property_codes_data()

        # Show the database structure
        #self.show_db_structure()

        # Export the database structure to an Excel file
        self.export_db_structure_to_excel()

        self.conn.close()




if __name__ == "__main__":

    db = community_solarDatabase()
    #db.clear_database()
    db.create_database_and_build()
    #db.get_and_insert_solar_data()


