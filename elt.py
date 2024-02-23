# Secrets
import os

# Time
from datetime import datetime

# AWS
import boto3

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

# This process aims to align with the latest and greatest practices of Data Engineering through the use of a Medallion Architecture 

# Database credentials and connection setup
DATABASE_URL = os.environ.get('DATABASE_URL')
engine = create_engine(DATABASE_URL)

Session = sessionmaker(bind=engine)

# Reflect existing db tables into SQLAlchemy
Base = automap_base()
Base.prepare(autoload_with=engine)

# Selecting Postgres Tables as Python Objects via SQLAlchemy
accidents_bronze = Base.classes.accidents_bronze
accidents_silver = Base.classes.accidents_silver
log = Base.classes.log

# Getting secrets necessary for Amazon Geocoding Services
aws_access_key_id = os.environ.get('aws_access_key_id')
aws_secret_access_key = os.environ.get('aws_secret_access_key')
region_name = os.environ.get('region_name')
service_name = os.environ.get('service_name')
place_index_name = os.environ.get('place_index_name')

# Establishing connection to Amazon Web Services SDK
client = boto3.client(
    service_name=service_name,
    region_name=region_name,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

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
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        })
        try:
            response = session.get(url)
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                tables = soup.find_all('table', {'class': 'us_acc_table'})
                all_data = []

                for table in tables:
                    season_header = table.find_previous_sibling('h2')
                    season = season_header.text.split(' SEASON')[0] if season_header else 'Season Unknown'

                    for row in table.find_all('tr')[1:]:
                        cells = row.find_all('td')
                        if len(cells) >= 5:
                            accident_data = {
                                'season': season,
                                'date': cells[0].text.strip(),
                                'state': cells[1].text.strip(),
                                'location': cells[2].text.strip(),
                                'description': cells[3].text.strip(),
                                'fatalities': cells[4].text.strip()
                            }
                            all_data.append(accident_data)
                return all_data, None  # Return the data and indicate no error
            else:
                # If status_code is not 200, construct an error message
                error_message = f"Failed to fetch the webpage. Status code: {response.status_code}"
                return None, error_message  # Return no data and the error message
        except Exception as e:
            # Handle any other exceptions that may occur and construct an error message
            error_message = f"An error occurred while trying to scrape and parse {url}: {str(e)}"
            return None, error_message  # Return no data and the error message
        
        
# Returns full date from season and partial date
def transform_date(row):
    date_text = row['date']
    season = row['season']
    
    try:
        month, day = map(int, date_text.split('/'))
        year = int(season.split('-')[0]) if month > 6 else int(season.split('-')[1])
        return datetime(year, month, day).strftime('%Y-%m-%d')
    except Exception as e:
        error_message = f"An error occurred while trying to transform date {row}: {str(e)}"
        return None, error_message
    

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


def geocode_address(place_index_name, location_text):
    try:
        # Call Amazon Location Service to geocode the location
        response = client.search_place_index_for_text(
            IndexName=place_index_name,  # The name of your place index resource
            Text=location_text  # Combined location and state
        )
        # Check if the response contains any results
        if not response['Results']:
            return None
        # Extract latitude and longitude from response
        coordinates = response['Results'][0]['Place']['Geometry']['Point']
        return coordinates
    except Exception as e:
        error_message = f"An error has occurred while geocoding: {str(e)}"
        return None, error_message 
    
    
def split_coordinates(row):
    if row['coordinates']:
        # Directly assign longitude and latitude in the order they're returned
        return pd.Series(row['coordinates'], index=['longitude', 'latitude'])
    else:
        return pd.Series((None, None), index=['longitude', 'latitude'])
    

# URL to scrape from (permitted for research purposes: https://avalanche.state.co.us/accidents/statistics-and-reporting)
url = "https://classic.avalanche.state.co.us/caic/acc/acc_us.php"

# Scrape new data
new_data, error_message = scrape_and_parse(url)

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

        # Step 2: Select the Updated Bronze Table Containing All Records Including Recently Inserted
        df_bronze = pd.read_sql("SELECT * FROM accidents_bronze", con=engine)
        
        # Step 3: Remove cross symbols if they exist
        df_bronze['date'] = df_bronze['date'].astype(str).str.replace("†", "", regex=False)
        
        # Step 4: Transform 'date' column based on 'season' 
        df_bronze['date'] = df_bronze.apply(transform_date, axis=1)
        
        # Step 5: Create a Silver DataFrame for Transformations
        # Remove summary tables and apply transformations
        headers_to_look_for = {'date', 'state', 'location', 'description', 'fatalities', 'season'}
        silver_df = df_bronze[df_bronze.columns.intersection(headers_to_look_for)]
              
        # Step 6: Convert state abbreviations to full names
        silver_df.loc[:, 'state'] = silver_df['state'].apply(lambda x: us.states.lookup(x).name if us.states.lookup(x) else x)
        
        # Step 7: Geocode each location to obtain coordinates
        silver_df['coordinates'] = silver_df.apply(lambda x: geocode_address(place_index_name, f"{x['location']}, {x['state']}"), axis=1)
            
        # Step 8: Split coordinates into latitude and longitude
        silver_df[['longitude', 'latitude']] = silver_df.apply(lambda row: split_coordinates(row), axis=1)
               
        # Step 9: Drop the temporary 'coordinates' column
        silver_df.drop(columns=['coordinates'], inplace=True)
             
        # Step 10: Inserting Transformed Data into Silver Table
        append_to_sql(silver_df, 'accidents_silver', engine)
        
        db_session.commit()

        # Set count of records
        data_count = len(new_df)
        success = True
    else:
        # Can still be a successful run with no data to bring in
        success = True

except Exception as e:
    error_message = f"An Error has occurred during the transformation stage: {str(e)}"
    db_session.rollback()  # Rollback any pending transactions

finally:
    # This block executes whether there was an error or not
    status = 'Success' if success else 'Failure'
    end_time = datetime.now()
    duration = end_time - start_time

    if success:
        # Log Success
        insert_log(db_session, elt_job_id, start_time, end_time, duration, status, data_count)
    else:
        # Log Failure  
        insert_log(db_session, elt_job_id, start_time, end_time, duration, status, data_count, error_message)
    
    db_session.close()  # Always close the session