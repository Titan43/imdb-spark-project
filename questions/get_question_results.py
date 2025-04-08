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
from .questions2 import (
    get_comedies_after_2010,
    get_top10_rated,
    get_drama_directors,
    get_breaking_bad_episodes,
    get_actors_with_most_series,
    get_top10_genres_by_film_count
)
from .questions3 import (
    get_animation_count_by_decade,
    get_longest_active_people,
    get_top_animated_title_by_year,
    get_top_animation_directors,
    compare_originals_and_sequels,
    get_top_franchises_by_rating_count
)
from .questions4 import (
    get_most_frequent_actor_director_pairs,
    get_tv_series_with_highest_avg_episode_ratings,
    get_actors_with_most_genre_diversity,
    get_top_rated_movies_by_new_directors,
    get_genre_trends_over_time,
    get_most_versatile_directors
)

from . import CACHE_PATH

questions_implementation = {
    "genres_by_avg_runtime":get_genres_by_avg_runtime,
    "genres_by_dominating_long_running_series":get_genres_by_dominating_long_running_series,
    "years_with_most_releases":get_years_with_most_releases,
    "directors_with_most_movies":get_directors_with_most_movies,
    "most_common_genres_by_top_directors":get_most_common_genres_by_top_directors,
    "top_most_consistent_directors":get_top_most_consistent_directors,
    "comedies_after_2010":get_comedies_after_2010,
    "top10_rated":get_top10_rated,
    "drama_directors":get_drama_directors,
    "breaking_bad_episodes":get_breaking_bad_episodes,
    "actors_with_most_series":get_actors_with_most_series,
    "top10_genres_by_film_count":get_top10_genres_by_film_count,
    "get_animation_count_by_decade": get_animation_count_by_decade,
    "get_longest_active_people": get_longest_active_people,
    "get_top_animated_title_by_year": get_top_animated_title_by_year,
    "get_top_animation_directors": get_top_animation_directors,
    "compare_originals_and_sequels": compare_originals_and_sequels,
    "get_top_franchises_by_rating_count": get_top_franchises_by_rating_count,
    "most_frequent_actor_director_pairs": get_most_frequent_actor_director_pairs,
    "tv_series_with_highest_avg_episode_ratings": get_tv_series_with_highest_avg_episode_ratings,
    "actors_with_most_genre_diversity": get_actors_with_most_genre_diversity,
    "top_rated_movies_by_new_directors": get_top_rated_movies_by_new_directors,
    "genre_trends_over_time": get_genre_trends_over_time,
    "most_versatile_directors": get_most_versatile_directors
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