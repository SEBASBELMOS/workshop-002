import logging
import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from dotenv import load_dotenv
import os
import time

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

def extract_spotify_api_data(artist_names):
    """
    Extract artist data (name and followers) from Spotify API for a list of artists, and save to the data folder.
    
    Args:
        artist_names (list): List of artist names to search for.
    
    Returns:
        pd.DataFrame: DataFrame containing artist data (name and followers).
    """
    logger.info(f"Extracting Spotify artist data for {len(artist_names)} artists")
    artists_data = []
    artist_ids = []
    artist_name_mapping = {}
    
    for idx, artist_name in enumerate(artist_names):
        try:
            logger.info(f"Searching for artist {idx + 1}/{len(artist_names)}: {artist_name}")
            artist_results = sp.search(q=f"artist:{artist_name}", type="artist", limit=1)
            if not artist_results or not artist_results["artists"]["items"]:
                logger.warning(f"No artist found for: {artist_name}")
                artists_data.append({"artist_name": artist_name, "followers": None})
                continue
            
            artist = artist_results["artists"]["items"][0]
            artist_ids.append(artist["id"])
            artist_name_mapping[artist["id"]] = artist["name"]
            
            time.sleep(0.05)
        
        except Exception as e:
            logger.error(f"Error searching for artist {artist_name}: {e}")
            artists_data.append({"artist_name": artist_name, "followers": None})
            continue
    
    batch_size = 50
    for i in range(0, len(artist_ids), batch_size):
        batch_ids = artist_ids[i:i + batch_size]
        try:
            logger.info(f"Fetching details for artist batch {i // batch_size + 1}/{(len(artist_ids) // batch_size) + 1}")
            artists_batch = sp.artists(batch_ids)
            for artist in artists_batch["artists"]:
                artist_data = {
                    "artist_name": artist_name_mapping[artist["id"]],
                    "followers": artist["followers"]["total"]
                }
                artists_data.append(artist_data)
            
            time.sleep(0.05)
        
        except Exception as e:
            logger.error(f"Error fetching artist batch: {e}")
            for artist_id in batch_ids:
                artists_data.append({"artist_name": artist_name_mapping.get(artist_id, "Unknown"), "followers": None})
            continue
    
    artist_df = pd.DataFrame(artists_data)
    logger.info(f"Extracted data for {len(artist_df)} artists from Spotify API")
    logger.info(f"Spotify artist sample data:\n{artist_df.head(2).to_string()}")

    data_dir = "/opt/airflow/data"
    os.makedirs(data_dir, exist_ok=True)
    
    artist_file_path = os.path.join(data_dir, "spotify_artists_followers.csv")
    artist_df.to_csv(artist_file_path, index=False)
    logger.info(f"Saved artist data to {artist_file_path}")

    return artist_df