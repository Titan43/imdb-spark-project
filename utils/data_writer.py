import os
from typing import Optional
from pyspark.sql import DataFrame

class DataWriter:
    def __init__(self, cache_dir: str):
        self.cache_dir = cache_dir
        os.makedirs(self.cache_dir, exist_ok=True)

    def save_as_csv(self, df: DataFrame, filename: str, mode: str = "overwrite") -> Optional[str]:
        """
        Saves the provided DataFrame as a compressed CSV file (Gzip) in the cache directory.

        :param df: PySpark DataFrame to save
        :param filename: Filename without extension
        :param mode: Write mode ('overwrite', 'append', etc.)
        :return: Full path to the saved CSV
        """
        output_path = os.path.join(self.cache_dir, f"{filename}.csv.gz")

        try:
            df.coalesce(1).write \
                .option("header", "true") \
                .option("compression", "gzip") \
                .mode(mode) \
                .csv(output_path)
            print(f"DataFrame saved (compressed) to {output_path}")
            return output_path
        except Exception as e:
            print(f"Error saving DataFrame to compressed CSV: {e}")
            return None