import pandas as pd
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def transform_spotify_api_data(spotify_df):
    """
    Transform Spotify API artist data by cleaning and normalizing it.
    
    Args:
        spotify_df (pd.DataFrame): DataFrame containing raw Spotify API artist data (artist_name, followers).
    
    Returns:
        pd.DataFrame: Transformed DataFrame.
    """
    logger.info("Transforming Spotify API artist data")
    try:
        df = spotify_df.copy()
        
        df.fillna({
            "artist_name": "Unknown",
            "followers": 0
        }, inplace=True)
        
        df.columns = [col.lower().replace(" ", "_") for col in df.columns]
        
        logger.info(f"Transformed Spotify API artist data with {len(df)} rows")
        logger.info(f"Transformed Spotify API artist sample data:\n{df.head(2).to_string()}")
        return df
    except Exception as e:
        logger.error(f"Error transforming Spotify API artist data: {e}", exc_info=True)
        raise