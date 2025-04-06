import pandas as pd
import requests
import io
import os

CACHE_DIR = "/app/.cache"

def load_tsv_gz_from_urls(url: str):
    """
    Loads .tsv.gz files from a URL into a pandas DataFrame.
    If the file has been previously cached, it loads from the cache instead of downloading it again.

    :param url: URL pointing to the .tsv.gz file
    :return: pandas DataFrame
    """
    filename = url.split('/')[-1].replace(".tsv.gz", "")
    cache_path = os.path.join(CACHE_DIR, f"{filename}.tsv.gz")
    
    if os.path.exists(cache_path):
        print(f"Loading cached dataset: {filename}")
        return pd.read_csv(cache_path, sep="\t", compression='gzip', low_memory=False)

    print(f"Downloading {filename}...")
    try:
        response = requests.get(url)
        response.raise_for_status()
        
        os.makedirs(CACHE_DIR, exist_ok=True)
        with open(cache_path, 'wb') as f:
            f.write(response.content)
        
        with io.BytesIO(response.content) as gz_data:
            df = pd.read_csv(gz_data, sep="\t", compression='gzip', low_memory=False)
            print(f"Loaded: {filename} with shape {df.shape}")
        
        return df
    except Exception as e:
        print(f"Error: failed to load file {filename} — {e}")
        return None
