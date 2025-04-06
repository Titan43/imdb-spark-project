from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from .data_downloader import Downloader

class DataLoader:
    def __init__(self, spark_session: SparkSession, cache_dir: str):
        self.spark_session = spark_session
        self.cache_dir = cache_dir

    def load_data(self, url: str) -> Optional[DataFrame]:
        """
        Downloads the .tsv.gz file (if not cached) and loads it into a PySpark DataFrame.

        :param url: URL pointing to the .tsv.gz file
        :return: PySpark DataFrame
        """
        downloader = Downloader(self.cache_dir)
        cache_path = downloader.download_file(url)
        
        if cache_path:
            print(f"Loading data from {cache_path} into DataFrame...")
            return self.spark_session.read.option("delimiter", "\t").csv(cache_path, header=True, inferSchema=True)
        else:
            print("Failed to load data.")
            return None
