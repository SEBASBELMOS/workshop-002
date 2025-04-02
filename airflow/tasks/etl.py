from src.extract.spotify_extract import extract_spotify_data
from src.extract.grammys_extract import extract_grammys_data

from src.transform.spotify_transform import transform_spotify_data
from src.transform.grammys_transform import transform_grammys_data
from src.transform.merge import merge_data

from src.load_store.load import load_data
from src.load_store.store import store_merged_data

import os
import json
import pandas as pd
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%d/%m/%Y %I:%M:%S %p"
)

def extract_spotify():
    try:
        df = extract_spotify_data("./data/spotify_dataset.csv") 
        return df.to_json(orient="records")
    except Exception as e:
        logging.error(f"Error extracting data: {e}")

def extract_grammys():
    try:
        df = extract_grammys_data()
        return df.to_json(orient="records")
    except Exception as e:
        logging.error(f"Error extracting data: {e}")

def transform_spotify(df):
    try:
        json_df = json.loads(df)
        
        raw_df = pd.DataFrame(json_df)
        df = transform_spotify_data(raw_df)
        
        return df.to_json(orient="records")
    except Exception as e:
        logging.error(f"Error transforming data: {e}")

def transform_grammys(df):
    try:
        json_df = json.loads(df)
        
        raw_df = pd.DataFrame(json_df)
        df = transform_grammys_data(raw_df)
        
        return df.to_json(orient="records")
    except Exception as e:
        logging.error(f"Error transforming data: {e}")

def merge_data(spotify_df, grammys_df):
    try:
        spotify_json = json.loads(spotify_df)
        grammys_json = json.loads(grammys_df)
        
        spotify_df = pd.DataFrame(spotify_json)
        grammys_df = pd.DataFrame(grammys_json)
        
        df = merge_data(spotify_df, grammys_df)
        
        return df.to_json(orient="records")
    except Exception as e:
        logging.error(f"Error merging data: {e}")

def load_data(df):
    try:
        json_df = json.loads(df)
        
        df = pd.DataFrame(json_df)
        load_data(df, "merged_data")
        
        return df.to_json(orient="records")
    except Exception as e:
        logging.error(f"Error loading data: {e}")

def store_data(df):
    try:
        json_df = json.loads(df)
        
        df = pd.DataFrame(json_df)
        store_data("merged_data", df)
    except Exception as e:
        logging.error(f"Error storing data: {e}")
        