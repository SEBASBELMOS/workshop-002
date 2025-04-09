import logging
import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from dotenv import load_dotenv
import os

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

load_dotenv("/opt/airflow/.env")

SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")

try:
    sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
        client_id=SPOTIFY_CLIENT_ID,
        client_secret=SPOTIFY_CLIENT_SECRET
    ))
    logger.info("Spotify API authentication successful")
except Exception as e:
    logger.error(f"Failed to authenticate with Spotify API: {e}")
    raise

def extract_spotify_api_data(artist_name="The Beatles"):
    """
    Extract track data from Spotify API for a given artist.
    
    Args:
        artist_name (str): Name of the artist to search for.
    
    Returns:
        pd.DataFrame: DataFrame containing Spotify track data.
    """
    logger.info(f"Extracting Spotify data for artist: {artist_name}")
    try:
        results = sp.search(q=f"artist:{artist_name}", type="track", limit=50)
        
        if not results or not results["tracks"]["items"]:
            raise ValueError(f"No tracks found for artist: {artist_name}")
        
        tracks = []
        for item in results["tracks"]["items"]:
            track = {
                "track_name": item["name"],
                "artist_name": item["artists"][0]["name"],
                "album_name": item["album"]["name"],
                "release_date": item["album"]["release_date"],
                "track_id": item["id"],
                "popularity": item["popularity"],
                "duration_ms": item["duration_ms"],
                "explicit": item["explicit"]
            }
            tracks.append(track)
        
        df = pd.DataFrame(tracks)
        logger.info(f"Extracted {len(df)} tracks from Spotify API")
        logger.info(f"Spotify sample data:\n{df.head(2).to_string()}")
        return df
    except Exception as e:
        logger.error(f"Error extracting Spotify API data: {e}", exc_info=True)
        raise