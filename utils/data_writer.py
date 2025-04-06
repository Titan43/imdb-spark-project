import os
from pyspark.sql import DataFrame

class DataWriter:
    def __init__(self, cache_dir: str):
        self.cache_dir = cache_dir
        os.makedirs(self.cache_dir, exist_ok=True)

    def save_as_csv(self, df: DataFrame, filename: str, mode: str = "overwrite"):
        """
        Saves the provided DataFrame as a CSV file in the cache directory.

        :param df: PySpark DataFrame to save
        :param filename: Filename without extension
        :param mode: Write mode ('overwrite', 'append', etc.)
        :return: Full path to the saved CSV
        """
        output_path = os.path.join(self.cache_dir, f"{filename}.csv")

        try:
            df.coalesce(1).write.option("header", "true").mode(mode).csv(output_path)
            print(f"DataFrame saved to {output_path}")
            return output_path
        except Exception as e:
            print(f"Error saving DataFrame to CSV: {e}")
            return None
