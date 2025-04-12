import pandas as pd
import logging
from typing import Dict, List, Union, Optional
import json

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%d/%m/%Y %I:%M:%S %p")
log = logging.getLogger(__name__)

def categorise_duration(duration_ms: int) -> str:
    if duration_ms < 150000:
        return "Short"
    elif 150000 <= duration_ms <= 300000:
        return "Average"
    else:
        return "Long"

def categorise_popularity(popularity: int) -> str:
    if popularity <= 30:
        return "Low Popularity"
    elif 31 <= popularity <= 70:
        return "Average Popularity"
    else:
        return "High Popularity"

def determine_mood(valence: float) -> str:
    if valence <= 0.3:
        return "Sad"
    elif 0.31 <= valence <= 0.6:
        return "Neutral"
    else:
        return "Happy"

def extract_primary_artist(artists: Optional[str]) -> Optional[str]:
    if pd.isna(artists):
        return None
    artists = artists.replace(" featuring ", ";").replace(" feat. ", ";").replace(" ft. ", ";")
    if ";" in artists:
        artists = artists.split(";")[0].strip()
    elif " & " in artists:
        artists = artists.split(" & ")[0].strip()
    elif "," in artists:
        artists = artists.split(",")[0].strip()
    if artists.lower().startswith("the "):
        artists = artists[4:].strip()
    artists = artists.replace("'", "").replace("!", "").replace("(", "").replace(")", "")
    return artists.strip()

def transform_spotify_data(df: Union[pd.DataFrame, str]) -> Optional[str]:
    try:
        if isinstance(df, str):
            try:
                df = pd.DataFrame(json.loads(df))
            except json.JSONDecodeError as e:
                log.error(f"Invalid JSON string provided: {str(e)}")
                return None

        if df.empty:
            raise ValueError("Input DataFrame is empty")
            
        log.info(f"Initial dimensions: {df.shape[0]} rows and {df.shape[1]} columns.")
        
        if "Unnamed: 0" in df.columns:
            df = df.drop(columns=["Unnamed: 0"])
            log.info(f"After dropping 'Unnamed: 0': {df.shape[0]} rows")
        
        df = df.dropna().reset_index(drop=True)
        log.info(f"After dropping null values: {df.shape[0]} rows")
        
        df = df.drop_duplicates()
        log.info(f"After drop_duplicates(): {df.shape[0]} rows")
        
        df = df.drop_duplicates(subset=["track_id"], keep="first").reset_index(drop=True)
        log.info(f"After drop_duplicates(track_id): {df.shape[0]} rows")
        
        df["artists"] = df["artists"].apply(extract_primary_artist)
        log.info(f"After extracting primary artist: {df.shape[0]} rows")
        log.info(f"Sample artists after extraction: {df['artists'].head().tolist()}")
        
        # df = (df
        #       .sort_values(by="popularity", ascending=False)
        #       .groupby(["track_name", "artists"])
        #       .head(1)
        #       .sort_index()
        #       .reset_index(drop=True))
        # log.info(f"After deduplication by track_name and artists: {df.shape[0]} rows")
        
        genre_mapping: Dict[str, List[str]] = {
            'Rock/Metal': [
                'alt-rock', 'alternative', 'black-metal', 'death-metal', 'emo', 'grindcore',
                'hard-rock', 'hardcore', 'heavy-metal', 'metal', 'metalcore', 'psych-rock',
                'punk-rock', 'punk', 'rock-n-roll', 'rock', 'grunge', 'j-rock', 'goth',
                'industrial', 'rockabilly', 'indie'
            ],
            'Pop': [
                'pop', 'indie-pop', 'power-pop', 'k-pop', 'j-pop', 'mandopop', "cantopop",
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
                'acoustic', 'classical', 'guitar', 'piano',
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
        log.info(f"After genre mapping: {df.shape[0]} rows")
        
        df["duration_min"] = df["duration_ms"].apply(lambda x: x // 60000)
        df["duration_min"] = df["duration_min"].astype(int)
        log.info(f"After calculating duration_min: {df.shape[0]} rows")
        
        df["duration_category"] = df["duration_ms"].apply(categorise_duration)
        df["popularity_category"] = df["popularity"].apply(categorise_popularity)
        df["track_mood"] = df["valence"].apply(determine_mood)
        df["live_performance"] = df["liveness"].apply(lambda x: True if x > 0.8 else False)
        log.info(f"After adding categorical columns: {df.shape[0]} rows")
        
        columns_to_drop = [
            "loudness", "mode", "duration_ms", "key", "tempo", "valence",
            "speechiness", "acousticness", "instrumentalness", "liveness",
            "time_signature"
        ]
        df = df.drop(columns=columns_to_drop)
        log.info(f"After dropping columns: {df.shape[0]} rows")
        
        if "artists" not in df.columns:
            raise KeyError("Expected 'artists' column in Spotify dataset DataFrame")

        log.info(f"Final dimensions: {df.shape[0]} rows and {df.shape[1]} columns.")
        
        return df.to_json(orient="records")
        
    except Exception as e:
        log.error(f"An error occurred during transformation: {str(e)}")
        return None