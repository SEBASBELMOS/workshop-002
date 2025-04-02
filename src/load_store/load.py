from src.database.db_operations import create_gcp_engine, load_data_clean, dispose_engine

import pandas as pd
import logging
from typing import Optional, Union
import json

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%d/%m/%Y %I:%M:%S %p")

def load_data(df: Union[pd.DataFrame, str], table_name: str = "merged_data", schema: str = "merged") -> Optional[str]:
    """
    Loads a DataFrame into the specified database table.
    
    This function handles the complete process of loading data into a database table,
    including engine creation, data loading, and proper cleanup. It provides detailed
    logging of the process and handles potential errors that might occur during
    database operations.
    
    Parameters:
        df (Union[pd.DataFrame, str]): The DataFrame to be loaded into the database.
                                     Can be either a DataFrame or a JSON string.
        table_name (str): The name of the table where the data will be loaded.
                         Defaults to "merged_data".
        schema (str): The database schema to load the data into.
                     Defaults to "merged".
    
    Returns:
        Optional[str]: JSON string representation of the loaded DataFrame if successful,
                      None if an error occurs.
        
    Raises:
        ValueError: If the input DataFrame is empty or if table_name is empty.
    """
    
    if isinstance(df, str):
        try:
            df = pd.DataFrame(json.loads(df))
        except json.JSONDecodeError as e:
            logging.error(f"Invalid JSON string provided: {str(e)}")
            return None
    
    if df.empty:
        raise ValueError("Input DataFrame is empty")
    if not table_name or not isinstance(table_name, str):
        raise ValueError("Invalid table name provided")
        
    logging.info(f"Starting to load clean data to table: {schema}.{table_name}")
    
    engine = create_gcp_engine()
    
    try:
        loaded_df = load_data_clean(engine, df, table_name, schema)
        logging.info(f"Successfully loaded {len(loaded_df)} rows to table: {schema}.{table_name}")
        return loaded_df.to_json(orient="records")
    except Exception as e:
        logging.error(f"Error loading clean data to the database: {str(e)}")
        return None
    finally:
        dispose_engine(engine)