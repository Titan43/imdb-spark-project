import pandas as pd
import requests
import io

def load_tsv_gz_from_urls(url: str):
    """
    Loads .tsv.gz files from a list of URLs into a dictionary of pandas DataFrames.

    :param urls: List of URLs pointing to .tsv.gz files
    :return: Dictionary where keys are filenames without extension, and values are pandas DataFrames
    """

    filename = url.split('/')[-1].replace(".tsv.gz", "")
    print(f"Downloading {filename}...")

    df = {}

    try:
        response = requests.get(url)
        response.raise_for_status()  
        with io.BytesIO(response.content) as gz_data:
            df = pd.read_csv(gz_data, sep="\t", compression='gzip', low_memory=False)
            print(f"Loaded: {filename} with shape {df.shape}")
    except Exception as e:
        print(f"Error: failed to load file {filename} — {e}")
    
    return df