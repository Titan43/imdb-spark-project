import os
import pandas as pd
import requests
import io

CACHE_DIR = "/app/.cache"
os.makedirs(CACHE_DIR, exist_ok=True)

def load_tsv_gz_from_url(url: str) -> pd.DataFrame:
    """
    Downloads and caches a .tsv.gz file from a URL and loads it as a pandas DataFrame.
    Caches the file locally to avoid re-downloading.

    :param url: URL pointing to a .tsv.gz file
    :return: pandas DataFrame
    """
    filename = url.split('/')[-1]
    cache_path = os.path.join(CACHE_DIR, filename)
    print(f"Preparing {filename}...")

    if os.path.exists(cache_path):
        print(f"Found cached file: {cache_path}")
        df = pd.read_csv(cache_path, sep="\t", compression='gzip', low_memory=False)
    else:
        try:
            print(f"Downloading {url}...")
            response = requests.get(url)
            response.raise_for_status()

            with open(cache_path, 'wb') as f:
                f.write(response.content)

            df = pd.read_csv(io.BytesIO(response.content), sep="\t", compression='gzip', low_memory=False)
            print(f"Downloaded and cached: {filename} with shape {df.shape}")
        except Exception as e:
            print(f"Error: Failed to download {filename} — {e}")
            df = pd.DataFrame()  

    return df
