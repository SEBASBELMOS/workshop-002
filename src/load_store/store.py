from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from dotenv import load_dotenv
import os
import pandas as pd
import logging
import json
from typing import Union

logging.basicConfig(
                    level=logging.INFO, 
                    format="%(asctime)s %(message)s", 
                    datefmt="%d/%m/%Y %I:%M:%S %p"
                    )

load_dotenv("/opt/airflow/.env")

client_secrets_file = os.getenv('CLIENT_SECRETS_PATH')
settings_file = os.getenv('SETTINGS_PATH')
credentials_file = os.getenv('SAVED_CREDENTIALS_PATH')
folder_id = os.getenv("FOLDER_ID")

def auth_drive():
    """
    Authenticates and returns a Google Drive instance using the PyDrive library.
    This function uses saved credentials for authentication. If no credentials are found,
    it raises an error instead of attempting interactive authentication.
    
    Returns:
        GoogleDrive: An authenticated GoogleDrive instance.
    Raises:
        FileNotFoundError: If required configuration files are missing.
        Exception: If there is an error during the authentication process.
    """
    try:
        logging.info("Starting Google Drive authentication process.")

        logging.info(f"Client secrets path: {client_secrets_file}")
        logging.info(f"Settings file path: {settings_file}")
        logging.info(f"Credentials file path: {credentials_file}")
        logging.info(f"Folder ID: {folder_id}")

        if not os.path.exists(client_secrets_file):
            raise FileNotFoundError(f"Client secrets file not found at {client_secrets_file}")
        if not os.path.exists(settings_file):
            raise FileNotFoundError(f"Settings file not found at {settings_file}")
        if not os.path.exists(credentials_file):
            raise FileNotFoundError(
                f"Saved credentials not found at {credentials_file}. "
                "Please generate credentials using generate_credentials.py on a local machine."
            )

        gauth = GoogleAuth(settings_file=settings_file)
        
        logging.info("Loading saved credentials.")
        gauth.LoadCredentialsFile(credentials_file)
        if gauth.access_token_expired:
            logging.info("Access token expired, refreshing token.")
            gauth.Refresh()
            gauth.SaveCredentialsFile(credentials_file)
            logging.info("Token refreshed and saved.")
        else:
            logging.info("Using saved credentials.")

        drive = GoogleDrive(gauth)
        logging.info("Google Drive authentication completed successfully.")
        return drive

    except Exception as e:
        logging.error(f"Authentication error: {e}", exc_info=True)
        raise

def store_merged_data(title: str, df: Union[pd.DataFrame, str]) -> None:
    """
    Stores a given DataFrame as a CSV file on Google Drive.
    
    Parameters:
        title (str): The title of the file to be stored on Google Drive.
        df (Union[pd.DataFrame, str]): The DataFrame to be stored as a CSV file.
                                     Can be either a DataFrame or a JSON string.
    
    Returns:
        None
    
    Raises:
        ValueError: If the input DataFrame is empty or if title is empty.
        Exception: If there is an error during the upload process.
    """
    try:
        if isinstance(df, str):
            try:
                df = pd.DataFrame(json.loads(df))
            except json.JSONDecodeError as e:
                logging.error(f"Invalid JSON string provided: {str(e)}")
                raise

        if df.empty:
            raise ValueError("Input DataFrame is empty")
        if not title or not isinstance(title, str):
            raise ValueError("Invalid title provided")

        drive = auth_drive()
        
        logging.info(f"Storing {title} on Google Drive.")
        logging.info(f"DataFrame has {len(df)} rows and {len(df.columns)} columns.")
        
        csv_file = df.to_csv(index=False) 
        
        file = drive.CreateFile({
            "title": title,
            "parents": [{"kind": "drive#fileLink", "id": folder_id}],
            "mimeType": "text/csv"
        })
        
        file.SetContentString(csv_file)
        
        file.Upload()
        
        logging.info(f"File {title} uploaded successfully.")

    except Exception as e:
        logging.error(f"Error storing data on Google Drive: {e}", exc_info=True)
        raise