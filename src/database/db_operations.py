import os
import logging
from typing import Any, Optional, Union
from dotenv import load_dotenv
from sqlalchemy import (
    create_engine, inspect, BigInteger, Boolean, Integer, Float,
    String, Text, DateTime, MetaData, Table, Column
)
from sqlalchemy.engine import Engine
from sqlalchemy_utils import database_exists, create_database
import pandas as pd
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv("/opt/airflow/.env")

def get_env_var(name: str) -> str:
    """
    Retrieves and validates an environment variable.
    
    Args:
        name (str): Name of the environment variable
        
    Returns:
        str: Value of the environment variable
        
    Raises:
        ValueError: If the environment variable is not set
    """
    value = os.getenv(name)
    if value is None:
        raise ValueError(f"Environment variable {name} is not set")
    return value

DB_CONFIG = {
    "user": get_env_var("PG_USER"),
    "password": get_env_var("PG_PASSWORD"),
    "host": get_env_var("PG_HOST"),
    "port": get_env_var("PG_PORT"),
    "database": get_env_var("PG_DATABASE")
}

try:
    port_num = int(DB_CONFIG["port"])
    if not (1 <= port_num <= 65535):
        raise ValueError(f"Invalid port number: {port_num}")
except ValueError as e:
    raise ValueError(f"Invalid port number in environment variables: {DB_CONFIG['port']}") from e


def create_gcp_engine() -> Engine:
    """
    Creates a database engine using environment variables.
    
    This function initialises a connection to the PostgreSQL database using
    credentials stored in environment variables.
    
    Returns:
        Engine: SQLAlchemy database engine instance
        
    Raises:
        Exception: If there is an error creating the database engine
    """
    try:
        db_url = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        
        engine = create_engine(db_url)
        logging.info("Database engine created successfully.")
        return engine
        
    except Exception as e:
        logging.error(f"Error creating database engine: {str(e)}")
        raise


def dispose_engine(engine: Engine) -> None:
    """
    Disposes of the database engine.
    
    This function properly closes and cleans up the database connection.
    
    Args:
        engine (Engine): SQLAlchemy database engine to dispose
        
    Raises:
        Exception: If there is an error disposing of the engine
    """
    try:
        engine.dispose()
        logging.info("Database engine disposed successfully.")
    except Exception as e:
        logging.error(f"Error disposing database engine: {str(e)}")
        raise


def infer_types(dtype: Any, column_name: str, df: pd.DataFrame) -> Union[Integer, Float, String, Text, DateTime, Boolean]:
    """
    Infers the appropriate SQLAlchemy type for a DataFrame column.
    
    Args:
        dtype (Any): Pandas dtype of the column
        column_name (str): Name of the column
        df (pd.DataFrame): DataFrame containing the column
        
    Returns:
        Union[Integer, Float, String, Text, DateTime, Boolean]: SQLAlchemy type
    """
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


def load_data_raw(engine: Engine, df: pd.DataFrame, table_name: str, schema: str = "raw") -> None:
    """
    Loads raw data from a DataFrame into a database table.
    
    This function creates or replaces a table with the raw data from the DataFrame.
    No type inference or schema validation is performed.
    
    Args:
        engine (Engine): SQLAlchemy database engine
        df (pd.DataFrame): DataFrame containing the data to load
        table_name (str): Name of the table to create or replace
        
    Raises:
        Exception: If there is an error creating or loading the table
    """
    logging.info(f"Creating table {schema}.{table_name} from Pandas DataFrame.")
    
    try:   
        with engine.connect() as conn:
            df.to_sql(table_name, con=conn, schema=schema, if_exists="replace", index=False)
            logging.info(f"Table {schema}.{table_name} was created successfully.")
    
    except Exception as e:
        logging.error(f"Error creating table {schema}.{table_name}: {str(e)}")
        raise


def load_data_clean(engine: Engine, df: pd.DataFrame, table_name: str, schema: str = "merged") -> None:
    """
    Loads cleaned data from a DataFrame into a database table.
    
    This function creates a table with appropriate column types if it doesn't exist,
    then loads the data. It performs type inference and schema validation.
    
    Args:
        engine (Engine): SQLAlchemy database engine
        df (pd.DataFrame): DataFrame containing the cleaned data to load
        table_name (str): Name of the table to create or update
        
    Raises:
        Exception: If there is an error creating or loading the table
    """
    logging.info(f"Creating table {schema}.{table_name} from Pandas DataFrame.")
    
    try:
        if not inspect(engine).has_table(table_name, schema=schema):
            metadata = MetaData()
            columns = [
                Column(
                    name,
                    infer_types(dtype, name, df),
                    primary_key=(name == "id")
                )
                for name, dtype in df.dtypes.items()
            ]
            
            table = Table(table_name, metadata, *columns, schema=schema)
            table.create(engine)
            
            logging.info(f"Table {schema}.{table_name} was created successfully.")

            with engine.connect() as conn:
                df.to_sql(table_name, con=conn, schema=schema, if_exists="append", index=False)
                logging.info(f"Data loaded to table {schema}.{table_name}.")
        else:
            logging.error(f"Table {schema}.{table_name} already exists.")
            raise ValueError(f"Table {schema}.{table_name} already exists. Use load_data_raw to replace it.")
        
        return df
            
    except Exception as e:
        logging.error(f"Error creating table {schema}.{table_name}: {str(e)}")
        raise