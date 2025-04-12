import pandas as pd
import logging
from fuzzywuzzy import fuzz

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%d/%m/%Y %I:%M:%S %p"
)
logger = logging.getLogger(__name__)

def fill_null_values(df, columns, value):
    """
    Fills null values in specified columns with a given value.
    
    Args:
        df (pd.DataFrame): The DataFrame to modify.
        columns (list): List of column names to fill.
        value: The value to fill nulls with.
    """
    for column in columns:
        if column in df.columns:
            df[column] = df[column].fillna(value)
        else:
            logger.warning(f"Column '{column}' not found in DataFrame. Skipping fill_null_values for this column.")

def drop_columns(df, columns):
    """
    Drops specified columns from the DataFrame.
    
    Args:
        df (pd.DataFrame): The DataFrame to modify.
        columns (list): List of column names to drop.
    """
    existing_columns = [col for col in columns if col in df.columns]
    if existing_columns:
        df.drop(columns=existing_columns, inplace=True)
    else:
        logger.debug(f"None of the columns {columns} found in DataFrame. No columns dropped.")

def fuzzy_match_track_title(row, grammys_df, threshold=85):
    """
    Perform fuzzy matching between a Spotify track name and Grammy titles.
    
    Args:
        row: A row from spotify_df with 'track_name_clean'.
        grammys_df: The Grammy DataFrame with 'title_clean'.
        threshold: Minimum fuzzy match score to consider a match.
    
    Returns:
        pd.Series: Best matching row from grammys_df, or None if no match.
    """
    track_name = row['track_name_clean']
    best_score = 0
    best_match = None
    
    for _, grammy_row in grammys_df.iterrows():
        title = grammy_row['title_clean']
        score = fuzz.token_sort_ratio(track_name, title)
        if score > best_score and score >= threshold:
            best_score = score
            best_match = grammy_row
    
    return best_match

def merge_data(spotify_df: pd.DataFrame, grammys_df: pd.DataFrame, spotify_api_df: pd.DataFrame = None, track_ids: list = None) -> pd.DataFrame:
    """
    Merge the Spotify and Grammy datasets using fuzzy matching on 'track_name' and 'title',
    and optionally add Spotify API data using artist_id.

    Args:
        spotify_df (pd.DataFrame): Transformed Spotify DataFrame with columns like 'track_id', 'track_name', and 'artists'.
        grammys_df (pd.DataFrame): Transformed Grammy DataFrame with columns like 'title', 'category', 'is_winner', 'artist'.
        spotify_api_df (pd.DataFrame, optional): Spotify API DataFrame with artist data ('track_id', 'artist_id', 'artist_name', 'followers').
        track_ids (list, optional): List of track IDs corresponding to the rows in spotify_api_df.

    Returns:
        pd.DataFrame: Merged DataFrame with combined data.
    """
    try:
        logger.info("Starting dataset merge.")
        
        if spotify_df.empty:
            raise ValueError("Spotify DataFrame is empty.")
        if grammys_df.empty:
            raise ValueError("Grammy DataFrame is empty.")
        
        logger.info(f"Initial Spotify dataset has {spotify_df.shape[0]} rows and {spotify_df.shape[1]} columns.")
        logger.info(f"Spotify DataFrame columns: {spotify_df.columns.tolist()}")
        logger.info(f"Initial Grammy dataset has {grammys_df.shape[0]} rows and {grammys_df.shape[1]} columns.")
        logger.info(f"Grammy DataFrame columns: {grammys_df.columns.tolist()}")
        if spotify_api_df is not None:
            if spotify_api_df.empty:
                logger.warning("Spotify API DataFrame is empty. Skipping API data merge.")
                spotify_api_df = None
            else:
                logger.info(f"Initial Spotify API dataset has {spotify_api_df.shape[0]} rows and {spotify_api_df.shape[1]} columns.")
                logger.info(f"Spotify API DataFrame columns: {spotify_api_df.columns.tolist()}")

        required_spotify_cols = ['track_id', 'track_name', 'artists']
        required_grammy_cols = ['title', 'artist']
        missing_spotify_cols = [col for col in required_spotify_cols if col not in spotify_df.columns]
        missing_grammy_cols = [col for col in required_grammy_cols if col not in grammys_df.columns]
        if missing_spotify_cols:
            raise KeyError(f"Missing required columns in Spotify DataFrame: {missing_spotify_cols}")
        if missing_grammy_cols:
            raise KeyError(f"Missing required columns in Grammy DataFrame: {missing_grammy_cols}")

        spotify_df = spotify_df.copy()
        grammys_df = grammys_df.copy()
        spotify_df['track_name_clean'] = spotify_df['track_name'].astype(str).str.lower().str.strip()
        grammys_df['title_clean'] = grammys_df['title'].astype(str).str.lower().str.strip()
        grammys_df['artist_clean'] = grammys_df['artist'].astype(str).str.lower().str.strip()
        spotify_df['artists_clean'] = spotify_df['artists'].astype(str).str.lower().str.strip()

        matched_rows = []
        for idx, spotify_row in spotify_df.iterrows():
            exact_match = grammys_df[
                (grammys_df['title_clean'] == spotify_row['track_name_clean']) &
                (grammys_df['artist_clean'] == spotify_row['artists_clean'])
            ]
            if not exact_match.empty:
                matched_row = exact_match.iloc[0]
            else:
                matched_row = fuzzy_match_track_title(spotify_row, grammys_df)
            
            if matched_row is not None:
                combined_row = spotify_row.to_dict()
                combined_row.update(matched_row.to_dict())
                matched_rows.append(combined_row)
            else:
                combined_row = spotify_row.to_dict()
                for col in grammys_df.columns:
                    if col not in combined_row:
                        combined_row[col] = None
                matched_rows.append(combined_row)

        df_merged = pd.DataFrame(matched_rows)
        logger.info(f"After merging Spotify and Grammy data: {df_merged.shape[0]} rows and {df_merged.shape[1]} columns.")

        fill_columns = ["year", "title", "category"]
        fill_null_values(df_merged, fill_columns, "Not applicable")

        fill_column = ["is_winner"]
        fill_null_values(df_merged, fill_column, False)

        columns_drop = ["artist_clean", "title_clean", "track_name_clean", "artists_clean"]
        drop_columns(df_merged, columns_drop)

        if 'id' not in df_merged.columns:
            df_merged = (df_merged
                         .reset_index()
                         .rename(columns={'index': 'id'}))
            df_merged['id'] = df_merged['id'].astype(int)
            logger.info("Added 'id' column to merged DataFrame.")

        if spotify_api_df is not None and track_ids is not None:
            logger.info("Processing Spotify API data for merging")
            
            spotify_api_df = spotify_api_df.copy()
            required_api_cols = ['track_id', 'artist_id', 'artist_name', 'followers']
            missing_api_cols = [col for col in required_api_cols if col not in spotify_api_df.columns]
            if missing_api_cols:
                logger.warning(f"Missing required columns {missing_api_cols} in Spotify API DataFrame. Skipping API data merge.")
            elif len(track_ids) != len(spotify_api_df):
                logger.warning(f"Mismatch between track_ids ({len(track_ids)}) and Spotify API DataFrame rows ({len(spotify_api_df)}). Skipping API data merge.")
            else:
                track_to_artist_map = dict(zip(spotify_api_df['track_id'], spotify_api_df['artist_id']))
                artist_to_followers_map = dict(zip(spotify_api_df['artist_id'], spotify_api_df['followers']))
                artist_to_name_map = dict(zip(spotify_api_df['artist_id'], spotify_api_df['artist_name']))

                df_merged['artist_id'] = df_merged['track_id'].map(track_to_artist_map)
                df_merged['followers'] = df_merged['artist_id'].map(artist_to_followers_map)
                df_merged['artist_name_api'] = df_merged['artist_id'].map(artist_to_name_map)

                logger.info(f"After merging with Spotify API data: {df_merged.shape[0]} rows and {df_merged.shape[1]} columns.")
                
                if 'followers' in df_merged.columns:
                    df_merged['followers'] = df_merged['followers'].fillna(0)
                    logger.info(f"Followers column filled with 0 for {df_merged['followers'].isna().sum()} rows.")
                
                if 'artist_name_api' in df_merged.columns and 'artists' in df_merged.columns:
                    df_merged['artist_match'] = df_merged['artists'].astype(str).str.lower() == df_merged['artist_name_api'].astype(str).str.lower()
                    match_rate = df_merged['artist_match'].mean() * 100
                    logger.info(f"Artist name match rate between Spotify dataset and API data: {match_rate:.2f}%")
                    df_merged.drop(columns=['artist_match'], inplace=True)
                
                if 'artist_name_api' in df_merged.columns:
                    df_merged.drop(columns=['artist_name_api'], inplace=True)

        if 'category' in df_merged.columns:
            num_matches = len(df_merged[df_merged['category'] != "Not applicable"])
            logger.info(f"Number of successful matches (Spotify tracks with Grammy data): {num_matches}")
            logger.info(f"Match rate: {num_matches / len(df_merged) * 100:.2f}%")
        else:
            logger.warning("Column 'category' not found in merged DataFrame. Cannot calculate match rate.")

        if 'artists' in df_merged.columns:
            unique_artists = df_merged['artists'].nunique()
            logger.info(f"Total unique artists in merged DataFrame: {unique_artists}")
            if 'followers' in df_merged.columns:
                artists_with_followers = df_merged[df_merged['followers'].notna()]['artists'].nunique()
                logger.info(f"Artists with follower data: {artists_with_followers} ({artists_with_followers / unique_artists * 100:.2f}% coverage)")

        logger.info(f"Merge process completed. The final DataFrame has {df_merged.shape[0]} rows and {df_merged.shape[1]} columns.")
        logger.info(f"Final DataFrame columns: {df_merged.columns.tolist()}")
        logger.info(f"Merged DataFrame sample:\n{df_merged.head(2).to_string()}")
        
        return df_merged
    
    except Exception as e:
        logger.error(f"An error occurred during the merge process: {e}", exc_info=True)
        raise