import pandas as pd
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def merge_data(spotify_df, grammys_df, spotify_api_df=None):
    """
    Merge Spotify, Grammy Awards, and Spotify API artist data.
    
    Args:
        spotify_df (pd.DataFrame): DataFrame containing Spotify dataset data.
        grammys_df (pd.DataFrame): DataFrame containing Grammy Awards data.
        spotify_api_df (pd.DataFrame, optional): DataFrame containing Spotify API artist data (artist_name, followers).
    
    Returns:
        pd.DataFrame: Merged DataFrame.
    """
    try:
        logger.info("Starting merge of Spotify, Grammy, and Spotify API artist data")
        
        logger.info(f"Spotify DataFrame columns: {spotify_df.columns.tolist()}")
        logger.info(f"Grammy DataFrame columns: {grammys_df.columns.tolist()}")
        if spotify_api_df is not None:
            logger.info(f"Spotify API DataFrame columns: {spotify_api_df.columns.tolist()}")
        
        if 'artist_name' not in spotify_df.columns:
            raise KeyError("Expected 'artist_name' column in Spotify DataFrame")
        if 'artist' not in grammys_df.columns:
            possible_artist_cols = ['nominee', 'artist_name', 'performer']
            artist_col = next((col for col in possible_artist_cols if col in grammys_df.columns), None)
            if artist_col:
                grammys_df = grammys_df.rename(columns={artist_col: 'artist'})
            else:
                raise KeyError("Expected 'artist' column in Grammy DataFrame; tried 'nominee', 'artist_name', 'performer'")
        
        spotify_df['artist_name'] = spotify_df['artist_name'].str.lower().str.strip()
        grammys_df['artist'] = grammys_df['artist'].str.lower().str.strip()
        
        merged_df = pd.merge(
            spotify_df,
            grammys_df,
            how='inner',
            left_on='artist_name',
            right_on='artist'
        )
        
        if spotify_api_df is not None:
            if 'artist_name' not in spotify_api_df.columns:
                raise KeyError("Expected 'artist_name' column in Spotify API DataFrame")
            spotify_api_df['artist_name'] = spotify_api_df['artist_name'].str.lower().str.strip()
            
            merged_df = pd.merge(
                merged_df,
                spotify_api_df[['artist_name', 'followers']],
                how='left',
                on='artist_name'
            )
        
        if merged_df.empty:
            logger.warning("No matches found after merging")
            logger.info(f"Spotify DataFrame sample:\n{spotify_df.head(2).to_string()}")
            logger.info(f"Grammy DataFrame sample:\n{grammys_df.head(2).to_string()}")
            if spotify_api_df is not None:
                logger.info(f"Spotify API DataFrame sample:\n{spotify_api_df.head(2).to_string()}")
        else:
            logger.info(f"Merged DataFrame has {len(merged_df)} rows")
            logger.info(f"Merged DataFrame sample:\n{merged_df.head(2).to_string()}")
        
        return merged_df
    except Exception as e:
        logger.error(f"Error merging data: {e}", exc_info=True)
        raise