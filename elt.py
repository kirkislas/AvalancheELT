# Secrets
import os

# Time
from datetime import datetime

# Geocoding
import googlemaps

# United States
import us

# Web Scraping
import requests
from bs4 import BeautifulSoup

# Data Manipulation
import pandas as pd

# Database Interaction
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base 


# Database credentials and connection setup
DATABASE_URL = os.environ.get('PROD_DATABASE_URL')
engine = create_engine(DATABASE_URL)

Session = sessionmaker(bind=engine)

# Reflect existing db tables into SQLAlchemy
Base = automap_base()
Base.prepare(autoload_with=engine)

# Selecting Postgres Tables as Python Objects via SQLAlchemy
accidents_bronze = Base.classes.accidents_bronze
accidents_silver = Base.classes.accidents_silver
log = Base.classes.log

google_maps_api_key = os.environ.get('GOOGLE_MAPS_API_KEY')
gmaps = googlemaps.Client(key=google_maps_api_key)

# Generated as a time stamp in seconds since 1970 for a unique reference (only if no others were created this day) 
elt_job_id = '1705574460'

# Assume failure unless proven otherwise
success = False

# Used for tracking errors if any 
error_message = None
 
# For tracking date of run and calculating duration
start_time = datetime.now()


def scrape_and_parse(url):
    with requests.Session() as session:
        session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        })
        try:
            response = session.get(url, timeout=30)

            if response.status_code != 200:
                error_message = (
                    f"Failed to fetch the webpage. Status code: {response.status_code}. "
                    f"First 200 chars: {response.text[:200]}"
                )
                return None, error_message

            soup = BeautifulSoup(response.content, "html.parser")
            tables = soup.find_all("table", {"class": "us_acc_table"})
            all_data = []

            if not tables:
                return None, "No tables found on page (us_acc_table missing). Page HTML may have changed."

            for table in tables:
                season_header = table.find_previous_sibling("h2")
                season = season_header.text.split(" SEASON")[0] if season_header else "Season Unknown"

                for row in table.find_all("tr")[1:]:
                    cells = row.find_all("td")
                    if len(cells) >= 5:
                        accident_data = {
                            "season": season,
                            "date": cells[0].text.strip(),
                            "state": cells[1].text.strip(),
                            "location": cells[2].text.strip(),
                            "description": cells[3].text.strip(),
                            "fatalities": cells[4].text.strip(),
                        }
                        all_data.append(accident_data)

            return all_data, None

        except Exception as e:
            error_message = f"An error occurred while trying to scrape and parse {url}: {str(e)}"
            return None, error_message

        
        
# Returns full date from season and partial date
def transform_date(row):
    date_text = row["date"]
    season = row["season"]

    try:
        month, day = map(int, str(date_text).split("/"))
        year = int(season.split("-")[0]) if month > 6 else int(season.split("-")[1])
        return datetime(year, month, day).strftime("%Y-%m-%d")
    except Exception:
        # Return a single value (None) so df.apply doesn't get tuples
        return None

    

def append_to_sql(df, table_name, engine):
    df.to_sql(table_name, con=engine, if_exists='append', index=False)


# This will run at the end of the script regardless of success or failure
def insert_log(db_session, elt_job_id, start_time, end_time, duration, status, data_count, error_message=None):
    # Defining the log entry
    log_entry = {
        "elt_job_id": elt_job_id,
        "start_time": start_time,
        "end_time": end_time,
        "duration": duration,
        "status": status,
        "data_count": data_count,
        "error_message": error_message
    }
    # Inserting log entry into the database
    db_session.execute(text("""
        INSERT INTO log (elt_job_id, start_time, end_time, duration, status, data_count, error_message)
        VALUES (:elt_job_id, :start_time, :end_time, :duration, :status, :data_count, :error_message)
        """), log_entry)
    db_session.commit()
    
    
def convert_state_abbreviation(abbreviation):
    state = us.states.lookup(abbreviation)
    return state.name if state else abbreviation


def refine_location(state, location):
    try:
        if not location:
            return None

        if "," in location:
            last_comma_part = location.split(",")[-1].strip()
            if " of " in last_comma_part:
                refined_location = last_comma_part.split(" of ", 1)[1]
            else:
                refined_location = last_comma_part
        elif " of " in location:
            refined_location = location.split(" of ", 1)[1]
        else:
            refined_location = location

        return f"{refined_location.strip()}, {state}"
    except Exception:
        return None



def geocode_location(location_text):
    try:
        # Use Google Maps to geocode the location
        geocode_result = gmaps.geocode(location_text)
        # Check if the geocode call returned results
        if geocode_result:
            # Extract latitude and longitude
            latitude = geocode_result[0]['geometry']['location']['lat']
            longitude = geocode_result[0]['geometry']['location']['lng']
            return latitude, longitude
        else:
            return None, None
    except Exception:
        return None, None


def invalidate_cache(api_endpoint, api_key):
    headers = {'access_token': api_key}
    # Assuming no specific cache keys are provided to clear the entire cache
    response = requests.post(api_endpoint, headers=headers, json={"keys": []})
    if response.status_code == 200:
        print("Cache invalidated successfully.")
    else:
        print(f"Failed to invalidate cache: {response.text}")
    

# URL to scrape from (permitted for research purposes: https://avalanche.state.co.us/accidents/statistics-and-reporting)
url = "https://classic.avalanche.state.co.us/caic/acc/acc_us.php"

# Scrape new data
new_data, error_message = scrape_and_parse(url)

# If scrape failed, log + exit cleanly (prevents len(None) crash)
if new_data is None:
    print(f"❌ Scrape failed: {error_message}")

    db_session = Session()
    try:
        status = "Failure"
        end_time = datetime.now()
        duration = end_time - start_time
        insert_log(
            db_session=db_session,
            elt_job_id=elt_job_id,
            start_time=start_time,
            end_time=end_time,
            duration=duration,
            status=status,
            data_count=0,
            error_message=error_message,
        )
    finally:
        db_session.close()

    raise SystemExit(1)

# Get count of new_data
new_data_count = len(new_data)


# Get count of current SQL bronze
current_bronze_count_query = "SELECT COUNT(*) FROM accidents_bronze"
current_bronze_count = pd.read_sql(current_bronze_count_query, engine).iloc[0, 0]

# Calculate difference in counts
difference = new_data_count - current_bronze_count

# If New Records to Add - Save New Records as DF - Else - Save DF as Empty
if new_data is not None and difference > 0:
    # There are new records to add
    new_df = pd.DataFrame(new_data[-difference:])  # Fetches only the new records
else:
    # No new records to add or the counts are identical
    new_df = pd.DataFrame()  # Empty DataFrame if no new data

db_session = Session()
data_count = 0

try:
    db_session.begin()
    
    if not new_df.empty:  # If there are records to add
        # Step 1: Insert Raw Data into Bronze Table
        append_to_sql(new_df, 'accidents_bronze', engine)
        print("Step 1: Raw data inserted into bronze table.")
        
        # Step 2: Select the Updated Bronze Table Containing All Records Including Recently Inserted
        df_bronze = pd.read_sql("SELECT * FROM accidents_bronze", con=engine)
        print("Step 2: Data selected from bronze table. Shape:", df_bronze.shape)
        
        print("Starting data transformation...")
        
        # Step 3: Remove cross symbols if they exist
        df_bronze['date'] = df_bronze['date'].astype(str).str.replace("†", "", regex=False)
        print("Step 3: Cross symbols removed. Sample data:", df_bronze['date'].head())
        
        # Step 4: Transform 'date' column based on 'season'
        df_bronze['date'] = df_bronze.apply(transform_date, axis=1)
        print("Step 4: Date transformed. Sample data:", df_bronze['date'].head())
        
        # Step 5: Create a Silver DataFrame for Transformations
        headers_to_look_for = {'date', 'state', 'location', 'description', 'fatalities', 'season'}
        silver_df = df_bronze[df_bronze.columns.intersection(headers_to_look_for)]
        print("Step 5: Silver DataFrame created. Shape:", silver_df.shape)
        
        # Step 6: Convert state abbreviations to full names
        silver_df.loc[:, 'state'] = silver_df['state'].apply(lambda x: us.states.lookup(x).name if us.states.lookup(x) else x)
        print("Step 6: State names converted. Sample data:", silver_df['state'].head())
        
        # Step 7: Refine Location for Geocoding purposes
        silver_df['refined_location'] = silver_df.apply(lambda x: refine_location(x['state'], x['location']), axis=1)
        print("Step 7: Locations refined. Sample data:", silver_df['refined_location'].head())
        
        # Step 8: Geocode each refined location to obtain latitude and longitude directly
        silver_df[['latitude', 'longitude']] = silver_df.apply(lambda x: pd.Series(geocode_location(x['refined_location'])), axis=1)
        print("Step 8: Geocoding completed. Sample data:", silver_df[['latitude', 'longitude']].head())
        
        # Step 9: Inserting Transformed Data into Silver Table
        append_to_sql(silver_df, 'accidents_silver', engine)
        print("Step 9: Transformed data inserted into silver table.")
        
        db_session.commit()
        print("Data transformation and loading completed successfully.")
        
        # Set count of records
        data_count = len(new_df)
        success = True
    else:
        print("No new data to process.")
        success = True

except Exception as e:
    print(f"An Error has occurred during the transformation stage: {e}")
    db_session.rollback()


finally:
    # This block executes whether there was an error or not
    status = 'Success' if success else 'Failure'
    end_time = datetime.now()
    duration = end_time - start_time

    if success:
        fast_api_endpoint = 'https://avalanchebackend.onrender.com/api/invalidate-cache/'
        api_key = os.environ.get('PROD_FAST_API_KEY')
        invalidate_cache(fast_api_endpoint, api_key)
        # Log Success
        insert_log(db_session, elt_job_id, start_time, end_time, duration, status, data_count)
    else:
        # Log Failure  
        insert_log(db_session, elt_job_id, start_time, end_time, duration, status, data_count, error_message)
    
    db_session.close()  # Always close the session