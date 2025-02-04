import requests
import json
from datetime import datetime
from snowflake.snowpark import Session
import sys
import pytz
import logging

# Initiate logging at info level
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(levelname)s - %(message)s')

# Set the IST time zone
ist_timezone = pytz.timezone('Asia/Kolkata')

# Get the current time in IST
current_time_ist = datetime.now(ist_timezone)

# Format the timestamp and file name
timestamp = current_time_ist.strftime('%Y_%m_%d_%H_%M_%S')
file_name = f'air_quality_data_{timestamp}.json'
today_string = current_time_ist.strftime('%Y_%m_%d')

# Snowflake connection setup
def snowpark_basic_auth() -> Session:
    connection_parameters = {
        "ACCOUNT": "ZD73737.ap-southeast-1",  # Include region in account name
        "USER": "DENY7",
        "PASSWORD": "Snowflake@1",
        "ROLE": "SYSADMIN",
        "DATABASE": "dev_db",
        "SCHEMA": "stage_sch",
        "WAREHOUSE": "load_wh"
    }
    return Session.builder.configs(connection_parameters).create()

def get_air_quality_data(api_key, limit):
    api_url = 'https://api.data.gov.in/resource/3b01bcb8-0b14-4abf-b6f2-c1bfd384ba69'
    params = {'api-key': api_key, 'format': 'json', 'limit': limit}
    headers = {'accept': 'application/json'}

    try:
        response = requests.get(api_url, params=params, headers=headers)
        logging.info('API response received')
        
        if response.status_code == 200:
            json_data = response.json()
            logging.info('JSON data parsed')
            
            with open(file_name, 'w') as json_file:
                json.dump(json_data, json_file, indent=2)
            logging.info(f'File saved locally: {file_name}')
            
            stg_location = f'@dev_db.stage_sch.raw_stg/india/{today_string}/'
            try:
                sf_session = snowpark_basic_auth()
                sf_session.file.put(file_name, stg_location)
                logging.info(f'File uploaded to stage: {stg_location}')
                
                lst_query = f'list {stg_location}{file_name}.gz'
                result_lst = sf_session.sql(lst_query).collect()
                logging.info(f'Stage file verified: {result_lst}')
                
                return json_data
            except Exception as e:
                logging.error(f"Snowflake error: {e}")
                sys.exit(1)
        else:
            logging.error(f"API error: {response.status_code} - {response.text}")
            sys.exit(1)
    except Exception as e:
        logging.error(f"General error: {e}")
        sys.exit(1)

# Execute the script
api_key = '579b464db66ec23bdd0000010f224c5eeb5248575974dac2ee7714c0'
limit_value = 4000
air_quality_data = get_air_quality_data(api_key, limit_value)
