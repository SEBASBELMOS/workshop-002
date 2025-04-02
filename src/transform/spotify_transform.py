import pandas as pd
import logging
from typing import Dict, List, Union, Optional
import json

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%d/%m/%Y %I:%M:%S %p")
log = logging.getLogger(__name__)


def categorise_duration(duration_ms: int) -> str:
    """
    Categorises the duration of a song based on its duration in milliseconds.
    
    Args:
        duration_ms (int): Duration of the song in milliseconds
        
    Returns:
        str: Duration category ("Short", "Average", or "Long")
    """
    if duration_ms < 150000:
        return "Short"
    elif 150000 <= duration_ms <= 300000:
        return "Average"
    else:
        return "Long"
    
def categorise_popularity(popularity: int) -> str:
    """
    Categorises the popularity of a song based on its popularity score.
    
    Args:
        popularity (int): Popularity score of the song (0-100)
        
    Returns:
        str: Popularity category ("Low Popularity", "Average Popularity", or "High Popularity")
    """
    if popularity <= 30:
        return "Low Popularity"
    elif 31 <= popularity <= 70:
        return "Average Popularity"
    else:
        return "High Popularity"
    
def determine_mood(valence: float) -> str:
    """
    Determines the mood of a song based on its valence score.
    
    Args:
        valence (float): Valence score of the song (0.0-1.0)
        
    Returns:
        str: Mood category ("Sad", "Neutral", or "Happy")
    """
    if valence <= 0.3:
        return "Sad"
    elif 0.31 <= valence <= 0.6:
        return "Neutral"
    else:
        return "Happy"
    
def transform_spotify_data(df: Union[pd.DataFrame, str]) -> Optional[str]:
    """
    Cleans and transforms the Spotify DataFrame.
    
    This function performs various data cleaning and transformation operations on the Spotify dataset,
    including removing duplicates, categorising durations and popularity, determining moods,
    and standardising genres. It handles both DataFrame and JSON string inputs.
    
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
            
        logging.info(f"Cleaning and transforming the DataFrame. Current dimensions: {df.shape[0]} rows and {df.shape[1]} columns.")
        
        df = df.drop(columns=["Unnamed: 0"])
       
        df = (df
                .dropna()
                .reset_index(drop=True))
        
        df = df.drop_duplicates()
        
        df = (df
                .drop_duplicates(subset=["track_id"])
                .reset_index(drop=True))
        
        genre_mapping: Dict[str, List[str]] = {
            'Rock/Metal': [
                'alt-rock', 'alternative', 'black-metal', 'death-metal', 'emo', 'grindcore',
                'hard-rock', 'hardcore', 'heavy-metal', 'metal', 'metalcore', 'psych-rock',
                'punk-rock', 'punk', 'rock-n-roll', 'rock', 'grunge', 'j-rock', 'goth',
                'industrial', 'rockabilly', 'indie'
            ],
            
            'Pop': [
                'pop', 'indie-pop', 'power-pop', 'k-pop', 'j-pop', 'mandopop', 'cantopop',
                'pop-film', 'j-idol', 'synth-pop'
            ],
            
            'Electronic/Dance': [
                'edm', 'electro', 'electronic', 'house', 'deep-house', 'progressive-house',
                'techno', 'trance', 'dubstep', 'drum-and-bass', 'dub', 'garage', 'idm',
                'club', 'dance', 'minimal-techno', 'detroit-techno', 'chicago-house',
                'breakbeat', 'hardstyle', 'j-dance', 'trip-hop'
            ],
            
            'Urban': [
                'hip-hop', 'r-n-b', 'dancehall', 'reggaeton', 'reggae'
            ],
            
            'Latino': [
                'brazil', 'salsa', 'samba', 'spanish', 'pagode', 'sertanejo',
                'mpb', 'latin', 'latino'
            ],
            
            'Global Sounds': [
                'indian', 'iranian', 'malay', 'turkish', 'tango', 'afrobeat', 'french', 'german', 'british', 'swedish'
            ],
            
            'Jazz and Soul': [
                'blues', 'bluegrass', 'funk', 'gospel', 'jazz', 'soul', 'groove', 'disco', 'ska'
            ],
            
            'Varied Themes': [
                'children', 'disney', 'forro', 'kids', 'party', 'romance', 'show-tunes',
                'comedy', 'anime'
            ],
            
            'Instrumental': [
                'acoustic', 'classical',  'guitar', 'piano',
                'world-music', 'opera', 'new-age'
            ],
            
            'Mood': [
                'ambient', 'chill', 'happy', 'sad', 'sleep', 'study'
            ],
            
            'Single Genre': [
                'country', 'honky-tonk', 'folk', 'singer-songwriter'
            ]
        }
        
        genre_category_mapping = {genre: category for category, genres in genre_mapping.items() for genre in genres}
        df["track_genre"] = df["track_genre"].map(genre_category_mapping)
        
        subset_cols = [col for col in df.columns if col not in ["track_id", "album_name"]]
        df = df.drop_duplicates(subset=subset_cols, keep="first")
        
        df = (df
                .sort_values(by="popularity", ascending=False)
                .groupby(["track_name", "artists"])
                .head(1)
                .sort_index()
                .reset_index(drop=True))
        
        df["duration_min"] = (df["duration_ms"]
                                .apply(lambda x: f"{x // 60000}"))
        
        df["duration_min"] = df["duration_min"].astype(int)

        df["duration_category"] = df["duration_ms"].apply(categorise_duration)
        
        df["popularity_category"] = df["popularity"].apply(categorise_popularity)
        
        df["track_mood"] = df["valence"].apply(determine_mood)
        
        df["live_performance"] = df["liveness"] > 0.8
        
        columns_to_drop = [
            "loudness", "mode", "duration_ms", "key", "tempo", "valence",
            "speechiness", "acousticness", "instrumentalness", "liveness",
            "time_signature"
        ]
        df = df.drop(columns=columns_to_drop)

        logging.info(f"The DataFrame has been cleaned and transformed. Final dimensions: {df.shape[0]} rows and {df.shape[1]} columns.")
        
        return df.to_json(orient="records")
        
    except Exception as e:
        logging.error(f"An error occurred during transformation: {str(e)}")
        return None