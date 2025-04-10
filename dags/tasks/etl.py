import sys
import os
import logging
from dotenv import load_dotenv
from sqlalchemy import create_engine
import pandas as pd
import json

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%d/%m/%Y %I:%M:%S %p"
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

logger.info(f"ETL tasks: Current working directory: {os.getcwd()}")
logger.info(f"ETL tasks: Initial Python path: {sys.path}")

sys.path.append('/opt/airflow/dags')
sys.path.append('/opt/airflow/src')
logger.info(f"ETL tasks: Updated Python path: {sys.path}")

from src.extract.spotify_extract import extract_spotify_data
from src.extract.grammys_extract import extract_grammys_data
from src.transform.spotify_transform import transform_spotify_data
from src.transform.grammys_transform import transform_grammys_data
from src.transform.merge import merge_data as merge_data_func
from src.load_store.load import load_data as load_data_func
from src.load_store.store import store_merged_data as store_data_func
from src.extract.extract_api import extract_spotify_api_data
from src.transform.transform_api import transform_spotify_api_data

load_dotenv("/opt/airflow/.env")

def create_schemas(**context):
    logger.info("DEBUG: create_schemas() called")
    try:
        db_user = os.getenv("PG_USER")
        db_password = os.getenv("PG_PASSWORD")
        db_host = os.getenv("PG_HOST")
        db_port = os.getenv("PG_PORT")
        db_name = os.getenv("PG_DATABASE")
        db_driver = os.getenv("PG_DRIVER")

        connection_string = f"{db_driver}://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        engine = create_engine(connection_string)
        logger.info("Database engine created successfully for schema creation.")

        schemas = ['raw', 'staging', 'processed']

        with engine.connect() as connection:
            for schema in schemas:
                connection.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
                logger.info(f"Ensured '{schema}' schema exists in the database.")

        engine.dispose()
        logger.info("Database engine disposed after schema creation.")

    except Exception as e:
        logger.error(f"Error creating schemas: {e}", exc_info=True)
        raise

def load_grammys_csv_to_db(**context):
    logger.info("DEBUG: load_grammys_csv_to_db() called")
    try:
        file_path = "/opt/airflow/data/the_grammy_awards.csv"
        logger.info(f"Loading Grammy Awards data from {file_path}")

        df = pd.read_csv(file_path)
        if df.empty:
            raise ValueError("No data found in the_grammy_awards.csv")

        logger.info(f"Loaded {len(df)} rows from the_grammy_awards.csv")
        logger.info(f"Sample data:\n{df.head(2).to_string()}")

        db_user = os.getenv("PG_USER")
        db_password = os.getenv("PG_PASSWORD")
        db_host = os.getenv("PG_HOST")
        db_port = os.getenv("PG_PORT")
        db_name = os.getenv("PG_DATABASE")
        db_driver = os.getenv("PG_DRIVER")

        connection_string = f"{db_driver}://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        engine = create_engine(connection_string)
        logger.info("Database engine created successfully.")

        df.to_sql(
            name='grammy_awards',
            schema='raw',
            con=engine,
            if_exists='replace',
            index=False
        )
        logger.info("Successfully loaded data into raw.grammy_awards table.")

        engine.dispose()
        logger.info("Database engine disposed.")

    except Exception as e:
        logger.error(f"Error loading Grammy Awards data into database: {e}", exc_info=True)
        raise

def extract_spotify(**context):
    logger.info("DEBUG: extract_spotify() called")
    try:
        file_path = "/opt/airflow/data/spotify_dataset.csv"
        logger.info(f"Extracting Spotify data from {file_path}")
        df = extract_spotify_data(file_path)
        if df.empty:
            raise ValueError("No data extracted from Spotify dataset")
        logger.info(f"Extracted Spotify data with {len(df)} rows")
        logger.info(f"Spotify sample data:\n{df.head(2).to_string()}")
        return df.to_json(orient="records")
    except Exception as e:
        logger.error(f"Error extracting Spotify data: {e}", exc_info=True)
        raise

def extract_spotify_api(**context):
    logger.info("DEBUG: extract_spotify_api() called")
    try:
        artist_names = context['ti'].xcom_pull(key='grammy_artists', task_ids='extract_grammys')
        logger.info(f"Pulled artist names from XCom: {artist_names[:5] if artist_names else 'None'}")
        if not artist_names:
            raise ValueError("No artist names received from extract_grammys task")
        
        artist_df = extract_spotify_api_data(artist_names=artist_names)
        if artist_df.empty:
            raise ValueError("No artist data extracted from Spotify API")
        
        context['ti'].xcom_push(key='artist_data', value=artist_df.to_json(orient="records"))
        logger.info("Pushed artist_data to XCom")
        return artist_df.to_json(orient="records")
    except Exception as e:
        logger.error(f"Error extracting Spotify API data: {e}", exc_info=True)
        raise

def extract_grammys(**context):
    logger.info("DEBUG: extract_grammys() called")
    try:
        logger.info("Extracting Grammy Awards data from database")
        dataframes = extract_grammys_data()
        logger.info(f"Extracted Grammy Awards data with keys: {list(dataframes.keys())}")
        
        if 'grammy_awards' not in dataframes:
            raise ValueError("Expected 'grammy_awards' table not found in extracted data")
        
        df = dataframes['grammy_awards']
        if df.empty:
            raise ValueError("No data extracted from Grammy Awards database")
        
        possible_artist_cols = ['artist', 'nominee', 'artist_name', 'performer']
        artist_col = next((col for col in possible_artist_cols if col in df.columns), None)
        if not artist_col:
            raise KeyError("No artist column found in Grammy data; tried 'artist', 'nominee', 'artist_name', 'performer'")
        
        artist_names = df[artist_col].dropna().unique().tolist()
        logger.info(f"Extracted {len(artist_names)} unique artist names from Grammy data")
        logger.info(f"Sample artist names: {artist_names[:5]}")
        
        context['ti'].xcom_push(key='grammy_artists', value=artist_names)
        logger.info("Pushed grammy_artists to XCom")
        
        logger.info(f"Grammy DataFrame columns: {df.columns.tolist()}")
        logger.info(f"Extracted Grammy Awards data with {len(df)} rows")
        logger.info(f"Grammy sample data:\n{df.head(2).to_string()}")
        return df.to_json(orient="records")
    except Exception as e:
        logger.error(f"Error extracting Grammy Awards data: {e}", exc_info=True)
        raise

def transform_spotify(df, **context):
    logger.info("DEBUG: transform_spotify() called")
    try:
        logger.info(f"Received Spotify data for transformation: {df[:100]}...")
        logger.info("Transforming Spotify data")
        json_df = json.loads(df)
        raw_df = pd.DataFrame(json_df)
        transformed_data = transform_spotify_data(raw_df)
        json_data = transformed_data
        try:
            if isinstance(json_data, pd.DataFrame):
                json_data = json_data.to_json(orient="records")
            json_parsed = json.loads(json_data)
            logger.info(f"Transformed Spotify data with {len(json_parsed)} rows")
            logger.info(f"Transformed Spotify sample data:\n{pd.DataFrame(json_parsed).head(2).to_string()}")
        except Exception as e:
            logger.info(f"Transformed Spotify data successfully, but couldn't parse JSON: {e}")
        return json_data
    except Exception as e:
        logger.error(f"Error transforming Spotify data: {e}", exc_info=True)
        raise

def transform_spotify_api(df, **context):
    logger.info("DEBUG: transform_spotify_api() called")
    try:
        logger.info(f"Received Spotify API data for transformation: {df[:100]}...")
        logger.info("Transforming Spotify API data")
        json_df = json.loads(df)
        raw_df = pd.DataFrame(json_df)
        transformed_data = transform_spotify_api_data(raw_df)
        json_data = transformed_data
        try:
            if isinstance(json_data, pd.DataFrame):
                json_data = json_data.to_json(orient="records")
            json_parsed = json.loads(json_data)
            logger.info(f"Transformed Spotify API data with {len(json_parsed)} rows")
            logger.info(f"Transformed Spotify API sample data:\n{pd.DataFrame(json_parsed).head(2).to_string()}")
        except Exception as e:
            logger.info(f"Transformed Spotify API data successfully, but couldn't parse JSON: {e}")
        return json_data
    except Exception as e:
        logger.error(f"Error transforming Spotify API data: {e}", exc_info=True)
        raise

def transform_grammys(df, **context):
    logger.info("DEBUG: transform_grammys() called")
    try:
        logger.info(f"Received Grammy data for transformation: {df[:100]}...")
        logger.info("Transforming Grammy Awards data")
        json_df = json.loads(df)
        raw_df = pd.DataFrame(json_df)
        transformed_data = transform_grammys_data(raw_df)
        json_data = transformed_data
        try:
            if isinstance(json_data, pd.DataFrame):
                json_data = json_data.to_json(orient="records")
            json_parsed = json.loads(json_data)
            logger.info(f"Transformed Grammy Awards data with {len(json_parsed)} rows")
            logger.info(f"Transformed Grammy sample data:\n{pd.DataFrame(json_parsed).head(2).to_string()}")
        except Exception as e:
            logger.info(f"Transformed Grammy Awards data successfully, but couldn't parse JSON: {e}")
        return json_data
    except Exception as e:
        logger.error(f"Error transforming Grammy Awards data: {e}", exc_info=True)
        raise

def merge_data(spotify_df, grammys_df, spotify_api_df=None, **context):
    logger.info("DEBUG: merge_data() called")
    try:
        logger.info(f"Received Spotify data for merging: {spotify_df[:100]}...")
        logger.info(f"Received Grammy data for merging: {grammys_df[:100]}...")
        if spotify_api_df:
            logger.info(f"Received Spotify API data for merging: {spotify_api_df[:100]}...")
        
        logger.info("Merging Spotify and Grammy Awards data")
        spotify_json = json.loads(spotify_df)
        grammys_json = json.loads(grammys_df)
        spotify_df = pd.DataFrame(spotify_json)
        grammys_df = pd.DataFrame(grammys_json)
        
        if spotify_api_df:
            spotify_api_json = json.loads(spotify_api_df)
            spotify_api_df = pd.DataFrame(spotify_api_json)
            merged_data = merge_data_func(spotify_df, grammys_df, spotify_api_df)
        else:
            merged_data = merge_data_func(spotify_df, grammys_df)
            
        if isinstance(merged_data, pd.DataFrame):
            merged_data = merged_data.to_json(orient="records")
        try:
            json_parsed = json.loads(merged_data)
            logger.info(f"Merged data with {len(json_parsed)} rows")
            logger.info(f"Merged sample data:\n{pd.DataFrame(json_parsed).head(2).to_string()}")
        except Exception as e:
            logger.info(f"Data merged successfully, but couldn't parse JSON: {e}")
        return merged_data
    except Exception as e:
        logger.error(f"Error merging data: {e}", exc_info=True)
        raise

def load_data(df, **context):
    logger.info("DEBUG: load_data() called")
    try:
        logger.info(f"Received data for loading: {df[:100]}...")
        logger.info("Loading merged data into database")
        json_df = json.loads(df)
        df = pd.DataFrame(json_df)
        load_data_func(df, "merged_data")
        logger.info("Merged data loaded successfully")
        return df.to_json(orient="records")
    except Exception as e:
        logger.error(f"Error loading data: {e}", exc_info=True)
        raise

def store_data(df, **context):
    logger.info("DEBUG: store_data() called")
    try:
        logger.info(f"Received data for storing: {df[:100]}...")
        logger.info("Storing merged data")
        json_df = json.loads(df)
        df = pd.DataFrame(json_df)
        store_data_func("merged_data", df)
        logger.info("Merged data stored successfully")
    except Exception as e:
        logger.error(f"Error storing data: {e}", exc_info=True)
        raise