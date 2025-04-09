import pandas as pd
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def transform_spotify_api_data(spotify_df):
    """
    Transform Spotify API data by cleaning and normalizing it.
    
    Args:
        spotify_df (pd.DataFrame): DataFrame containing raw Spotify API data.
    
    Returns:
        pd.DataFrame: Transformed DataFrame.
    """
    logger.info("Transforming Spotify API data")
    try:
        df = spotify_df.copy()
        
        df.fillna({
            "track_name": "Unknown",
            "artist_name": "Unknown",
            "album_name": "Unknown",
            "release_date": "Unknown",
            "popularity": 0,
            "duration_ms": 0,
            "explicit": False
        }, inplace=True)
        
        df.columns = [col.lower().replace(" ", "_") for col in df.columns]
        
        df["release_date"] = pd.to_datetime(df["release_date"], errors="coerce")
        
        df["year"] = df["release_date"].dt.year
        
        logger.info(f"Transformed Spotify API data with {len(df)} rows")
        logger.info(f"Transformed Spotify sample data:\n{df.head(2).to_string()}")
        return df
    except Exception as e:
        logger.error(f"Error transforming Spotify API data: {e}", exc_info=True)
        raise