"""
Python Extract Transform Load Example
"""

# %%
import requests
import pandas as pd
from sqlalchemy import create_engine

def extract()-> dict:
    """ This API extracts data from
    http://universities.hipolabs.com
    """
    API_URL = "http://universities.hipolabs.com/search?country=United+States"
    data = requests.get(API_URL).json()
    return data

def transform(data:dict) -> pd.DataFrame:
    """ Transforms the dataset into desired structure and filters"""
    df = pd.DataFrame(data)
    print(f"Total Number of universities from API {len(data)}")
    df = df[df["name"].str.contains("California")]
    print(f"Number of universities in california {len(df)}")
    df['domains'] = [','.join(map(str, l)) for l in df['domains']]
    df['web_pages'] = [','.join(map(str, l)) for l in df['web_pages']]
    df = df.reset_index(drop=True)
    return df[["domains","country","web_pages","name"]]

def load(df:pd.DataFrame)-> None:
    """ Loads data into a sqllite database"""
    disk_engine = create_engine('sqlite:///my_lite_store.db')
    df.to_sql('cal_uni', disk_engine, if_exists='replace')

# %%
data = extract()
df = transform(data)
load(df)
print(df)

# Create a connection to the SQLite database
#disk_engine = create_engine('sqlite:///my_lite_store.db')

# Read the data from the 'cal_uni' table
df_loaded = pd.read_sql('SELECT * FROM cal_uni', disk_engine)

# Display the loaded DataFrame
print(df_loaded)
# %%import logging

# Create a logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create a file handler and a stream handler
file_handler = logging.FileHandler('etl.log')
stream_handler = logging.StreamHandler()

# Create a formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
stream_handler.setFormatter(formatter)

# Add the handlers to the logger
logger.addHandler(file_handler)
logger.addHandler(stream_handler)

def extract()-> dict:
    """ This API extracts data from
    http://universities.hipolabs.com
    """
    API_URL = "http://universities.hipolabs.com/search?country=United+States"
    try:
        data = requests.get(API_URL).json()
        logger.info('Data extracted successfully')
        return data
    except Exception as e:
        logger.error('Error extracting data: ' + str(e))
        return None

def transform(data:dict) -> pd.DataFrame:
    """ Transforms the dataset into desired structure and filters"""
    try:
        df = pd.DataFrame(data)
        logger.info(f"Total Number of universities from API {len(data)}")
        df = df[df["name"].str.contains("California")]
        logger.info(f"Number of universities in california {len(df)}")
        df['domains'] = [','.join(map(str, l)) for l in df['domains']]
        df['web_pages'] = [','.join(map(str, l)) for l in df['web_pages']]
        df = df.reset_index(drop=True)
        logger.info('Data transformed successfully')
        return df[["domains","country","web_pages","name"]]
    except Exception as e:
        logger.error('Error transforming data: ' + str(e))
        return None

def load(df:pd.DataFrame)-> None:
    """ Loads data into a sqllite database"""
    try:
        disk_engine = create_engine('sqlite:///my_lite_store.db')
        df.to_sql('cal_uni', disk_engine, if_exists='replace')
        logger.info('Data loaded successfully')
    except Exception as e:
        logger.error('Error loading data: ' + str(e))

# %%
data = extract()
df = transform(data)
load(df)
logger.info('ETL process completed')
print(df)

# Create a connection to the SQLite database
disk_engine = create_engine('sqlite:///my_lite_store.db')

# Read the data from the 'cal_uni' table
df_loaded = pd.read_sql('SELECT * FROM cal_uni', disk_engine)

# Display the loaded DataFrame
print(df_loaded)