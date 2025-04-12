import logging
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv("/opt/airflow/.env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%d/%m/%Y %I:%M:%S %p")

def load_spotify_data(csv_path="/opt/airflow/data/spotify_artists_followers.csv"):
    """
    Load artist data from a CSV file.
    
    Args:
        csv_path (str): Path to the CSV file.
    
    Returns:
        pd.DataFrame: DataFrame with artist data (track_id, artist_id, artist_name, followers).
    """
    try:
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV file not found at {csv_path}")
        
        df = pd.read_csv(csv_path)
        logging.info(f"Loaded data from {csv_path} with {len(df)} rows")
        
        logging.info(f"Columns in CSV: {df.columns.tolist()}")
        
        expected_columns = ["track_id", "artist_id", "artist_followers", "artist_popularity"]
        if not all(col in df.columns for col in expected_columns):
            raise ValueError(f"CSV file {csv_path} must contain columns: {expected_columns}")
        
        df = df.rename(columns={
            "artist_followers": "followers"
        })
        
        df["artist_name"] = None
        
        df = df[["track_id", "artist_id", "artist_name", "followers"]]
        
        return df
    except Exception as e:
        logging.error(f"Failed to load CSV {csv_path}: {e}")
        raise

def extract_spotify_api_data(track_ids, csv_path="/opt/airflow/data/spotify_artists_followers.csv"):
    """
    Extract artist data from a CSV file using a list of track IDs.
    
    Args:
        track_ids (list): List of track IDs to process.
        csv_path (str): Path to the CSV file.
    
    Returns:
        pd.DataFrame: DataFrame with artist data (track_id, artist_id, artist_name, followers).
    """
    if not isinstance(track_ids, (list, tuple)):
        raise TypeError(f"track_ids must be a list or tuple, got {type(track_ids)}: {track_ids}")
    
    logging.info(f"Extracting Spotify artist data for {len(track_ids)} track IDs from CSV at {csv_path}")
    
    try:
        df = load_spotify_data(csv_path)
        
        df_filtered = df[df['track_id'].isin(track_ids)].copy()
        logging.info(f"Found {len(df_filtered)} matching track IDs in the CSV")
        
        result_df = pd.DataFrame({'track_id': track_ids})
        result_df = result_df.merge(
            df_filtered,
            on='track_id',
            how='left'
        )
        
        missing_tracks = result_df[result_df['artist_id'].isna()]['track_id'].tolist()
        if missing_tracks:
            logging.warning(f"No artist data found for {len(missing_tracks)} track IDs: {missing_tracks[:5]}...")
        
        expected_columns = ["track_id", "artist_id", "artist_name", "followers"]
        result_df = result_df[expected_columns]
        
        logging.info(f"Returning data for {len(result_df)} tracks")
        return result_df
    
    except Exception as e:
        logging.error(f"Unexpected error in extract_spotify_api_data: {e}")
        raise