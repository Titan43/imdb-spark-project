from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql import SparkSession, DataFrame
from utils.data_loader import DataLoader
from utils import get_filename_from_url
from . import CACHE_PATH

def preload_dfs(spark: SparkSession, urls: List[str]) -> Dict[str, DataFrame]:
    df_s: Dict[str, DataFrame] = {}
    loader = DataLoader(spark, CACHE_PATH)

    def load_single(url: str):
        filename = get_filename_from_url(url)
        df = loader.load_data(url)
        return filename, df

    with ThreadPoolExecutor(max_workers=min(8, len(urls))) as executor:
        future_to_url = {executor.submit(load_single, url): url for url in urls}
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                filename, df = future.result()
                df_s[filename] = df
            except Exception as e:
                print(f"Failed to load from {url}: {e}")

    return df_s
