import logging
import pandas as pd
import requests
from spotipy import Spotify
from spotipy.oauth2 import SpotifyClientCredentials
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from requests.exceptions import ReadTimeout, ConnectionError
from spotipy.exceptions import SpotifyException
from dotenv import load_dotenv
import os

load_dotenv("/opt/airflow/.env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%d/%m/%Y %I:%M:%S %p")

def create_spotify_client():
    """
    Create and configure a Spotify client with retry logic using credentials from .env.
    
    Returns:
        Spotify: Configured Spotify client.
    """
    try:
        client_id = os.getenv("SPOTIFY_CLIENT_ID")
        client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")
        
        if not client_id or not client_secret:
            raise ValueError("SPOTIFY_CLIENT_ID or SPOTIFY_CLIENT_SECRET not found in .env file.")
        
        retries = Retry(
            total=5,
            backoff_factor=0.3,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET"],
            raise_on_status=True
        )
        adapter = HTTPAdapter(max_retries=retries)
        session = requests.Session()
        session.mount("https://", adapter)
        
        client_credentials_manager = SpotifyClientCredentials(
            client_id=client_id,
            client_secret=client_secret
        )
        spotify = Spotify(
            client_credentials_manager=client_credentials_manager,
            requests_session=session,
            requests_timeout=15
        )
        
        spotify.search(q='test', type='track', limit=1)
        logging.info("Spotify client configured and connection tested successfully.")
        return spotify
    except Exception as e:
        logging.error(f"Error configuring Spotify client: {e}")
        raise

def load_fallback_data(csv_path="/opt/airflow/data/spotify_artist_data.csv"):
    """
    Load artist data from a CSV file as a fallback.
    
    Args:
        csv_path (str): Path to the CSV file.
    
    Returns:
        pd.DataFrame: DataFrame with artist data (track_id, artist_id, artist_name, followers).
    """
    try:
        df = pd.read_csv(csv_path)
        logging.info(f"Loaded fallback data from {csv_path} with {len(df)} rows")
        expected_columns = ["track_id", "artist_id", "artist_name", "followers"]
        if not all(col in df.columns for col in expected_columns):
            raise ValueError(f"CSV file {csv_path} must contain columns: {expected_columns}")
        return df
    except Exception as e:
        logging.error(f"Failed to load fallback CSV {csv_path}: {e}")
        raise

def extract_spotify_api_data(track_ids, csv_path="/opt/airflow/data/spotify_artist_data.csv"):
    """
    Extract artist data from Spotify API using a list of track IDs, with a fallback to CSV on 429 errors.
    Uses caching to avoid redundant API calls for the same artist.
    Saves the extracted data to a CSV file.
    
    Args:
        track_ids (list): List of track IDs to process.
        csv_path (str): Path to the fallback CSV file.
    
    Returns:
        pd.DataFrame: DataFrame with artist data (track_id, artist_id, artist_name, followers).
    """
    logging.info(f"Extracting Spotify artist data for {len(track_ids)} track IDs")
    
    try:
        spotify = create_spotify_client()
        artist_data_results = []
        
        artist_details_cache = {}
        processed_artist_ids = set()
        track_to_artist_map = {} 
        
        BATCH_SIZE = 50
        REQUEST_DELAY_SECONDS = 0.5
        
        for i in range(0, len(track_ids), BATCH_SIZE):
            batch_track_ids = track_ids[i:i + BATCH_SIZE]
            logging.info(f"Fetching track details for batch {i//BATCH_SIZE + 1}: {len(batch_track_ids)} tracks")
            
            try:
                time.sleep(REQUEST_DELAY_SECONDS)
                track_results = spotify.tracks(batch_track_ids)
                if track_results and track_results["tracks"]:
                    for track in track_results["tracks"]:
                        if track and track["id"] and track["artists"]:
                            track_id = track["id"]
                            primary_artist = track["artists"][0] if track["artists"] else None
                            if primary_artist and primary_artist["id"]:
                                artist_id = primary_artist["id"]
                                artist_name = primary_artist["name"]
                                track_to_artist_map[track_id] = (artist_id, artist_name)
                            else:
                                logging.warning(f"No primary artist found for track ID: {track_id}")
                        else:
                            logging.warning(f"Invalid track data for track ID: {track.get('id', 'unknown')}")
            except (SpotifyException, ReadTimeout, ConnectionError) as e:
                if isinstance(e, SpotifyException) and e.http_status == 429:
                    logging.warning("Rate limit hit (429). Falling back to CSV file.")
                    return load_fallback_data(csv_path)
                logging.error(f"Error fetching track batch {i//BATCH_SIZE + 1}: {e}")
                if isinstance(e, SpotifyException) and e.http_status == 429:
                    retry_after = int(e.headers.get("Retry-After", 30))
                    logging.warning(f"Rate limit reached. Waiting {retry_after} seconds...")
                    time.sleep(retry_after + 2)
                else:
                    logging.warning("Skipping track batch due to error.")
                    time.sleep(2)
            except Exception as e:
                logging.error(f"Unexpected error fetching track batch {i//BATCH_SIZE + 1}: {e}")
                time.sleep(2)
        
        artist_ids_to_fetch = set(artist_id for artist_id, _ in track_to_artist_map.values())
        artist_ids_to_fetch_now = list(artist_ids_to_fetch - processed_artist_ids)
        logging.info(f"Need to fetch data for {len(artist_ids_to_fetch_now)} unique artists")
        
        if artist_ids_to_fetch_now:
            for j in range(0, len(artist_ids_to_fetch_now), BATCH_SIZE):
                batch_artist_ids = artist_ids_to_fetch_now[j:j + BATCH_SIZE]
                logging.debug(f"Fetching artist details for sub-batch {j//BATCH_SIZE + 1}: {len(batch_artist_ids)} artists")
                
                try:
                    time.sleep(REQUEST_DELAY_SECONDS)
                    artist_results = spotify.artists(batch_artist_ids)
                    if artist_results and artist_results["artists"]:
                        for artist in artist_results["artists"]:
                            if artist and artist["id"]:
                                artist_id = artist["id"]
                                followers = artist.get("followers", {}).get("total", None)
                                artist_details_cache[artist_id] = followers
                    processed_artist_ids.update(batch_artist_ids)
                except (SpotifyException, ReadTimeout, ConnectionError) as e:
                    if isinstance(e, SpotifyException) and e.http_status == 429:
                        logging.warning("Rate limit hit (429). Falling back to CSV file.")
                        return load_fallback_data(csv_path)
                    logging.error(f"Error fetching artist sub-batch {j//BATCH_SIZE + 1}: {e}")
                    processed_artist_ids.update(batch_artist_ids)
                    if isinstance(e, SpotifyException) and e.http_status == 429:
                        retry_after = int(e.headers.get("Retry-After", 30))
                        logging.warning(f"Rate limit reached. Waiting {retry_after} seconds...")
                        time.sleep(retry_after + 2)
                    else:
                        logging.warning("Skipping artist sub-batch due to error.")
                        time.sleep(2)
                except Exception as e:
                    logging.error(f"Unexpected error fetching artist sub-batch {j//BATCH_SIZE + 1}: {e}")
                    processed_artist_ids.update(batch_artist_ids)
                    time.sleep(2)
        
        for track_id, (artist_id, artist_name) in track_to_artist_map.items():
            followers = artist_details_cache.get(artist_id)
            artist_data_results.append({
                "track_id": track_id,
                "artist_id": artist_id,
                "artist_name": artist_name,
                "followers": followers
            })
        
        for track_id in set(track_ids) - set(track_to_artist_map.keys()):
            artist_data_results.append({
                "track_id": track_id,
                "artist_id": None,
                "artist_name": None,
                "followers": None
            })
            logging.warning(f"No artist data for track ID: {track_id}")
        
        df = pd.DataFrame(artist_data_results)
        logging.info(f"Extracted data for {len(df)} artists from API")
        
        try:
            df.to_csv(csv_path, index=False)
            logging.info(f"Saved extracted Spotify API data to {csv_path}")
        except Exception as e:
            logging.error(f"Failed to save Spotify API data to {csv_path}: {e}")
        
        return df
    
    except Exception as e:
        logging.error(f"Unexpected error in extract_spotify_api_data: {e}")
        raise