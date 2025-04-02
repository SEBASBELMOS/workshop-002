import pandas as pd
import re
import logging
from typing import Optional, Union, List
import json

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%d/%m/%Y %I:%M:%S %p")

# Constants
CATEGORIES: List[str] = [
    "Best Classical Vocal Soloist Performance",
    "Best Classical Vocal Performance",
    "Best Small Ensemble Performance (With Or Without Conductor)",
    "Best Classical Performance - Instrumental Soloist Or Soloists (With Or Without Orchestra)",
    "Most Promising New Classical Recording Artist",
    "Best Classical Performance - Vocal Soloist (With Or Without Orchestra)",
    "Best New Classical Artist",
    "Best Classical Vocal Soloist",
    "Best Performance - Instrumental Soloist Or Soloists (With Or Without Orchestra)",
    "Best Classical Performance - Vocal Soloist"
]

ROLES_OF_INTEREST: List[str] = [
    "artist",
    "artists",
    "composer",
    "conductor",
    "conductor/soloist",
    "choir director",
    "chorus master",
    "graphic designer",
    "soloist",
    "soloists",
    "ensembles"
]

def extract_artist(workers: Optional[str]) -> Optional[str]:
    """
    Extracts the artist name from the 'workers' column if it's within parentheses.
    
    Args:
        workers (Optional[str]): The workers string to extract artist from
        
    Returns:
        Optional[str]: The extracted artist name or None if not found
    """
    if pd.isna(workers):
        return None
    match = re.search(r'\((.*?)\)', workers)
    if match:
        return match.group(1)
    return None

def send_workers_to_artist(row: pd.Series) -> Optional[str]:
    """
    Moves the value from 'workers' to 'artist' if 'artist' is NaN and 'workers' doesn't contain ';' or ','.
    
    Args:
        row (pd.Series): The row containing artist and workers columns
        
    Returns:
        Optional[str]: The artist value to use
    """
    if pd.isna(row["artist"]) and pd.notna(row["workers"]):
        workers = row["workers"]
        if not re.search(r'[;,]', workers):
            return workers
    return row["artist"]

def semicolon_artist(workers: Optional[str], roles: List[str]) -> Optional[str]:
    """
    Extracts the first segment of 'workers' before the semicolon if it doesn't contain roles of interest.
    
    Args:
        workers (Optional[str]): The workers string to process
        roles (List[str]): List of roles to check against
        
    Returns:
        Optional[str]: The extracted artist name or None if not found
    """
    if pd.isna(workers):
        return None
    parts = workers.split(';')
    first_part = parts[0].strip()
    if ',' not in first_part and not any(role in first_part.lower() for role in roles):
        return first_part
    return None

def extract_roles(workers: Optional[str], roles: List[str]) -> Optional[str]:
    """
    Extracts names associated with specific roles from 'workers' and assigns them to 'artist'.
    
    Args:
        workers (Optional[str]): The workers string to process
        roles (List[str]): List of roles to check against
        
    Returns:
        Optional[str]: The extracted artist names or None if not found
    """
    if pd.isna(workers):
        return None
    roles_pattern = '|'.join(roles)
    pattern = r'([^;]+)\s*,\s*(?:' + roles_pattern + r')'
    matches = re.findall(pattern, workers, flags=re.IGNORECASE)
    return ", ".join(matches).strip() if matches else None

def transform_grammys_data(df: Union[pd.DataFrame, str]) -> Optional[str]:
    """
    Cleans and transforms the Grammy Awards data and returns the DataFrame as JSON.
    
    Args:
        df (Union[pd.DataFrame, str]): Input DataFrame or JSON string
        
    Returns:
        Optional[str]: Transformed DataFrame as JSON string or None if error occurs
        
    Raises:
        ValueError: If input DataFrame is empty or required columns are missing
    """
    try:
        if isinstance(df, str):
            try:
                df = pd.DataFrame(json.loads(df))
            except json.JSONDecodeError as e:
                logging.error(f"Invalid JSON string provided: {str(e)}")
                return None
        
        if df.empty:
            raise ValueError("Input DataFrame is empty")
            
        required_columns = ["winner", "nominee", "artist", "workers", "category", 
                          "published_at", "updated_at", "img"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        logging.info(f"Starting transformation. The DataFrame has {df.shape[0]} rows and {df.shape[1]} columns.")
        
        df = df.rename(columns={"winner": "is_winner"})
        
        df = df.drop(columns=["published_at", "updated_at", "img"])
        
        df = df.dropna(subset=["nominee"])
        
        both_null_values = df[df["artist"].isna() & df["workers"].isna()]
        both_filtered = both_null_values[both_null_values["category"].isin(CATEGORIES)]
        both_null_values = both_null_values.drop(both_filtered.index)
        df = df.drop(both_filtered.index)
        df.loc[both_null_values.index, "artist"] = both_null_values["nominee"]
        
        df["artist"] = df.apply(
            lambda row: extract_artist(row["workers"]) if pd.isna(row["artist"]) else row["artist"],
            axis=1
        )
        
        df["artist"] = df.apply(send_workers_to_artist, axis=1)
        
        df["artist"] = df.apply(
            lambda row: semicolon_artist(row["workers"], ROLES_OF_INTEREST)
            if pd.isna(row["artist"]) else row["artist"],
            axis=1
        )
        
        df["artist"] = df.apply(
            lambda row: extract_roles(row["workers"], ROLES_OF_INTEREST)
            if pd.isna(row["artist"]) else row["artist"],
            axis=1
        )
        
        df = df.dropna(subset=["artist"])
        
        df["artist"] = df["artist"].replace({"(Various Artists)": "Various Artists"})
        
        df = df.drop(columns=["workers"])
        
        logging.info(f"Transformation complete. The DataFrame now has {df.shape[0]} rows and {df.shape[1]} columns.")
        
        return df.to_json(orient="records")
    
    except Exception as e:
        logging.error(f"An error occurred during transformation: {str(e)}")
        return None