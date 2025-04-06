import threading
from typing import Dict
from pyspark.sql import DataFrame
from utils.data_writer import DataWriter
from .questions1 import (
    get_genres_by_avg_runtime,
    get_genres_by_dominating_long_running_series,
    get_years_with_most_releases,
    get_directors_with_most_movies,
    get_most_common_genres_by_top_directors,
    get_top_most_consistent_directors
)
from . import CACHE_PATH

questions_implementation = {
    "get_genres_by_avg_runtime":get_genres_by_avg_runtime,
    "get_genres_by_dominating_long_running_series":get_genres_by_dominating_long_running_series,
    "get_years_with_most_releases":get_years_with_most_releases,
    "get_directors_with_most_movies":get_directors_with_most_movies,
    "get_most_common_genres_by_top_directors":get_most_common_genres_by_top_directors,
    "get_top_most_consistent_directors":get_top_most_consistent_directors,
}

def run(df_s: Dict[str, DataFrame]):
    writer = DataWriter(CACHE_PATH)
    threads = []
    
    for key, func in questions_implementation.items():
        thread = threading.Thread(
            target=lambda f=func, k=key: writer.save_as_csv(f(df_s), k),
            name=f"save_thread_{key}",
            daemon=True
        )
        threads.append(thread)
        thread.start()
    
    for thread in threads:
        thread.join()