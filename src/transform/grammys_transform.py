import pandas as pd
import re
import logging
from typing import Optional, Union, List

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%d/%m/%Y %I:%M:%S %p")
log = logging.getLogger(__name__)

CATEGORIES: List[str] = [
    "Best Classical Vocal Soloist Performance",
    "Best Classical Vocal Performance",
    "Best Small Ensemble Performance (With Or Without Conductor)",
    "Best Classical Performance - Instrumental Soloist Or Soloists (With Or Without Orchestra)",
    "Most Promising New Classical Recording Artist",
    "Best Classical Performance - Vocal Soloist (With Or Without Orchestra)",
    "Best New Classical Artist",
    "Best Classical Vocal Soloist",
    "Best Performance - Instrumental Soloist Or Soloists (With Or Without Orchestra)",
    "Best Classical Performance - Vocal Soloist"
]

ROLES_OF_INTEREST: List[str] = [
    "artist",
    "artists",
    "composer",
    "conductor",
    "conductor/soloist",
    "choir director",
    "chorus master",
    "graphic designer",
    "soloist",
    "soloists",
    "ensembles"
]

def extract_artist(workers: Optional[str]) -> Optional[str]:
    if pd.isna(workers):
        return None
    match = re.search(r'\((.*?)\)', workers)
    if match:
        return match.group(1)
    return None

def send_workers_to_artist(row: pd.Series) -> Optional[str]:
    if pd.isna(row["artist"]) and pd.notna(row["workers"]):
        workers = row["workers"]
        if not re.search(r'[;,]', workers):
            return workers
    return row["artist"]

def semicolon_artist(workers: Optional[str], roles: List[str]) -> Optional[str]:
    if pd.isna(workers):
        return None
    parts = workers.split(';')
    first_part = parts[0].strip()
    if ',' not in first_part and not any(role in first_part.lower() for role in roles):
        return first_part
    return None

def extract_roles(workers: Optional[str], roles: List[str]) -> Optional[str]:
    if pd.isna(workers):
        return None
    roles_pattern = '|'.join(roles)
    pattern = r'([^;]+)\s*,\s*(?:' + roles_pattern + r')'
    matches = re.findall(pattern, workers, flags=re.IGNORECASE)
    return ", ".join(matches).strip() if matches else None

def clean_grammy_artist(artist: Optional[str]) -> Optional[str]:
    if pd.isna(artist):
        return None
    artist = artist.replace(" featuring ", ";").replace(" feat. ", ";").replace(" ft. ", ";")
    if ";" in artist:
        artist = artist.split(";")[0].strip()
    elif " & " in artist:
        artist = artist.split(" & ")[0].strip()
    elif "," in artist:
        artist = artist.split(",")[0].strip()
    if artist.lower().startswith("the "):
        artist = artist[4:].strip()
    artist = artist.replace("'", "").replace("!", "").replace("(", "").replace(")", "")
    return artist.strip()

def transform_grammys_data(df: pd.DataFrame) -> pd.DataFrame:
    try:
        log.info(f"Initial Grammy DataFrame has {len(df)} rows and {len(df.columns)} columns")
        log.info(f"Raw Grammy DataFrame columns: {df.columns.tolist()}")
        
        if len(df.columns) != len(set(df.columns)):
            duplicates = df.columns[df.columns.duplicated()].tolist()
            log.warning(f"Duplicate columns found in raw DataFrame: {duplicates}")
            df = df.copy()
            for col in duplicates:
                dup_indices = [i for i, x in enumerate(df.columns) if x == col]
                for idx, dup_idx in enumerate(dup_indices[1:], 1):
                    df.columns.values[dup_idx] = f"{col}_{idx}"

        columns_to_drop = ['published_at', 'updated_at', 'img']
        df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])
        
        df = df.dropna(subset=["nominee"])
        
        both_null = df[df["artist"].isna() & df["workers"].isna()]
        both_filtered = both_null[both_null["category"].isin(CATEGORIES)]
        both_null = both_null.drop(both_filtered.index)
        df = df.drop(both_filtered.index)
        
        df.loc[both_null.index, "artist"] = both_null["nominee"]
        
        df["artist"] = df.apply(
            lambda row: extract_artist(row["workers"]) if pd.isna(row["artist"]) else row["artist"],
            axis=1
        )
        
        df["artist"] = df.apply(send_workers_to_artist, axis=1)
        
        df["artist"] = df.apply(
            lambda row: semicolon_artist(row["workers"], ROLES_OF_INTEREST)
            if pd.isna(row["artist"]) else row["artist"],
            axis=1
        )
        
        df["artist"] = df.apply(
            lambda row: extract_roles(row["workers"], ROLES_OF_INTEREST)
            if pd.isna(row["artist"]) else row["artist"],
            axis=1
        )
        
        df = df.dropna(subset=["artist"])
        
        df["artist"] = df["artist"].replace({"(Various Artists)": "Various Artists"})
        
        df["artist"] = df["artist"].apply(clean_grammy_artist)
        
        df = df.drop(columns=["workers"])
        
        if 'title' in df.columns:
            log.warning("Column 'title' already exists in DataFrame; dropping it to avoid conflict")
            df = df.drop(columns=['title'])
        
        df = df.rename(columns={
            'nominee': 'title',
            'winner': 'is_winner'
        })
        
        required_columns = ['year', 'category', 'title', 'artist', 'is_winner']
        for col in required_columns:
            if col not in df.columns:
                raise KeyError(f"Expected column '{col}' in Grammy DataFrame")
        
        df = df.fillna({
            'year': 0,
            'artist': 'Unknown',
            'title': 'Unknown',
            'category': 'Unknown',
            'is_winner': False
        })
        
        df['is_winner'] = df['is_winner'].astype(bool)
        
        log.info(f"Transformed Grammy DataFrame has {len(df)} rows and {len(df.columns)} columns")
        log.info(f"Sample artists after cleaning: {df['artist'].head().tolist()}")
        
        return df
        
    except Exception as e:
        log.error(f"Error transforming Grammy data: {e}")
        raise