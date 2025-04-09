import pandas as pd
import logging
from typing import List, Union, Optional
import json

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%d/%m/%Y %I:%M:%S %p")
log = logging.getLogger(__name__)


def fill_null_values(df: pd.DataFrame, columns: List[str], value: Union[str, bool]) -> None:
    """
    Fills null values in specified columns with a given value.
    
    Args:
        df (pd.DataFrame): The DataFrame to modify
        columns (List[str]): List of column names to fill null values in
        value (Union[str, bool]): The value to fill null values with
        
    Returns:
        None: Modifies the DataFrame in place
    """
    for column in columns:
        df[column] = df[column].fillna(value)

def drop_columns(df: pd.DataFrame, columns: List[str]) -> None:
    """
    Drops specified columns from the DataFrame.
    
    Args:
        df (pd.DataFrame): The DataFrame to modify
        columns (List[str]): List of column names to drop
        
    Returns:
        None: Modifies the DataFrame in place
    """
    df.drop(columns=columns, inplace=True, errors="ignore")
    

def merge_data(spotify_df: Union[pd.DataFrame, str], grammys_df: Union[pd.DataFrame, str], spotify_api_df: Union[pd.DataFrame, str] = None) -> Optional[str]:
    """
    Merges the Spotify, Grammys, and Spotify API datasets based on "track_name" and "nominee".
    
    This function combines the Spotify, Grammys, and Spotify API datasets using a left join,
    cleaning and standardising the data in the process. It handles both DataFrame
    and JSON string inputs, and returns the merged result as a JSON string.
    
    Args:
        spotify_df (Union[pd.DataFrame, str]): Spotify dataset as DataFrame or JSON string
        grammys_df (Union[pd.DataFrame, str]): Grammys dataset as DataFrame or JSON string
        spotify_api_df (Union[pd.DataFrame, str], optional): Spotify API dataset as DataFrame or JSON string
        
    Returns:
        Optional[str]: Merged DataFrame as JSON string or None if error occurs
        
    Raises:
        ValueError: If input DataFrames are empty or required columns are missing
    """
    try:
        if isinstance(spotify_df, str):
            try:
                spotify_df = pd.DataFrame(json.loads(spotify_df))
            except json.JSONDecodeError as e:
                logging.error(f"Invalid JSON string provided for Spotify data: {str(e)}")
                return None
                
        if isinstance(grammys_df, str):
            try:
                grammys_df = pd.DataFrame(json.loads(grammys_df))
            except json.JSONDecodeError as e:
                logging.error(f"Invalid JSON string provided for Grammys data: {str(e)}")
                return None

        if spotify_api_df is not None:
            if isinstance(spotify_api_df, str):
                try:
                    spotify_api_df = pd.DataFrame(json.loads(spotify_api_df))
                except json.JSONDecodeError as e:
                    logging.error(f"Invalid JSON string provided for Spotify API data: {str(e)}")
                    return None

        if spotify_df.empty or grammys_df.empty:
            raise ValueError("One or both input DataFrames are empty")
            
        spotify_required = ["track_name"]
        grammys_required = ["nominee"]
        
        spotify_missing = [col for col in spotify_required if col not in spotify_df.columns]
        grammys_missing = [col for col in grammys_required if col not in grammys_df.columns]
        
        if spotify_missing:
            raise ValueError(f"Missing required columns in Spotify data: {spotify_missing}")
        if grammys_missing:
            raise ValueError(f"Missing required columns in Grammys data: {grammys_missing}")

        logging.info("Starting dataset merge.")
        logging.info(f"Initial Spotify dataset has {spotify_df.shape[0]} rows and {spotify_df.shape[1]} columns.")
        logging.info(f"Initial Grammys dataset has {grammys_df.shape[0]} rows and {grammys_df.shape[1]} columns.")
        
        spotify_df["track_name_clean"] = spotify_df["track_name"].str.lower().str.strip()
        grammys_df["nominee_clean"] = grammys_df["nominee"].str.lower().str.strip()

        df_merged = spotify_df.merge(
            grammys_df,
            how="left",
            left_on="track_name_clean",
            right_on="nominee_clean",
            suffixes=("", "_grammys")
        )

        if spotify_api_df is not None and not spotify_api_df.empty:
            logging.info(f"Spotify API dataset has {spotify_api_df.shape[0]} rows and {spotify_api_df.shape[1]} columns.")
            spotify_api_df["track_name_clean"] = spotify_api_df["track_name"].str.lower().str.strip()
            
            df_merged = df_merged.merge(
                spotify_api_df,
                how="left",
                left_on="track_name_clean",
                right_on="track_name_clean",
                suffixes=("", "_api")
            )

        fill_columns = ["title", "category"]
        fill_null_values(df_merged, fill_columns, "Not applicable")

        fill_column = ["is_winner"]
        fill_null_values(df_merged, fill_column, False)

        columns_drop = [
            "year", "artist",
            "nominee", "nominee_clean", "track_name_clean"
        ]
        drop_columns(df_merged, columns_drop)
        
        df_merged = (df_merged
                     .reset_index()
                     .rename(columns={'index': 'id'}))
        
        df_merged['id'] = df_merged['id'].astype(int)

        logging.info(f"Merge process completed. The final dataframe has {df_merged.shape[0]} rows and {df_merged.shape[1]} columns.")
        
        return df_merged.to_json(orient="records")
    
    except Exception as e:
        logging.error(f"An error occurred during the merge process: {str(e)}")
        return None