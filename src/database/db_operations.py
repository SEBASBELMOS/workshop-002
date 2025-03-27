import os
import logging
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, BigInteger, Boolean, Integer, Float, String, Text, DateTime, MetaData, Table, Column
from sqlalchemy_utils import database_exists, create_database
import pandas as pd

#Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#Load environment variables
load_dotenv("../env/.env")

user = os.getenv("PG_USER")
password = os.getenv("PG_PASSWORD")
host = os.getenv("PG_HOST")
port = os.getenv("PG_PORT")
database = os.getenv("PG_DATABASE")

def create_gcp_engine():
    url = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    try:
        engine = create_engine(url)
        logger.info("Successfully created GCP database engine")
        return engine
    except Exception as e:
        logger.error(f"Failed to create GCP database engine: {str(e)}")
        raise


def dispose_engine(engine):
    engine.dispose()
    logging.info("Engine disposed.")


def infer_types(dtype, column_name, df):
    if "int" in dtype.name:
        return Integer
    elif "float" in dtype.name:
        return Float
    elif "object" in dtype.name:
        max_len = df[column_name].astype(str).str.len().max()
        if max_len > 255:
            logging.info(f"Modifying column {column_name} to text due to length {max_len}.")
            return Text
        else:
            return String(255)
    elif "datetime" in dtype.name:
        return DateTime
    elif "bool" in dtype.name:
        return Boolean
    else:
        return Text


def load_data_raw(engine, df, table_name):
    
    logging.info(f"Creating table {table_name} from Pandas DataFrame.")
    
    try:   
        df.to_sql(table_name, con=engine, if_exists="replace", index=False)
    
        logging.info(f"Table {table_name} was created successfully.")
    
    except Exception as e:
        logging.error(f"Error creating table {table_name}: {e}")


def load_data_clean(engine, df, table_name):
    
    logging.info(f"Creating table {table_name} from Pandas DataFrame.")
    
    try:
        if not inspect(engine).has_table(table_name):
            metadata = MetaData()
            columns = [Column(name,
                            infer_types(dtype, name, df),
                            primary_key=(name == "id")) \
                                for name, dtype in df.dtypes.items()]
            
            table = Table(table_name, metadata, *columns)
            table.create(engine)
            
            logging.info(f"Table {table_name} was created successfully.")

            df.to_sql(table_name, con=engine, if_exists="append", index=False)

            logging.info(f"Data loaded to table {table_name}.")
        else:
            logging.error(f"Table {table_name} already exists.")
    except Exception as e:
        logging.error(f"Error creating table {table_name}: {e}")