from typing import Dict
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, split, explode, countDistinct, avg, row_number, floor
)
from pyspark.sql.window import Window


def get_most_frequent_actor_director_pairs(df_s: Dict[str, DataFrame]):
    principals = df_s["title.principals"]
    crew = df_s["title.crew"]
    names = df_s["name.basics"]

    actors = principals.filter(col("category").isin("actor", "actress")).select("tconst", "nconst").withColumnRenamed("nconst", "actor_nconst")
    directors = crew.withColumn("director", explode(split(col("directors"), ","))).select("tconst", "director")

    df_pairs = actors.join(directors, "tconst", "inner")
    df_named = df_pairs.join(names.withColumnRenamed("nconst", "actor_nconst").withColumnRenamed("primaryName", "actor_name"), "actor_nconst")\
                       .join(names.withColumnRenamed("nconst", "director").withColumnRenamed("primaryName", "director_name"), "director")

    df_collab_counts = df_named.groupBy("actor_name", "director_name").count()
    window_spec = Window.orderBy(col("count").desc())
    df_ranked = df_collab_counts.withColumn("rank", row_number().over(window_spec)).filter(col("rank") <= 20)
    
    df_ranked.show()
    return df_ranked

def get_tv_series_with_highest_avg_episode_ratings(df_s: Dict[str, DataFrame]):
    basics = df_s["title.basics"]
    episodes = df_s["title.episode"]
    ratings = df_s["title.ratings"]

    df_episodes = episodes.join(ratings, "tconst", "inner")
    df_with_parents = df_episodes.groupBy("parentTconst").agg(avg("averageRating").alias("avg_rating"))
    df_joined = df_with_parents.join(basics, df_with_parents.parentTconst == basics.tconst)
    
    df_top_series = df_joined.select("primaryTitle", "avg_rating").orderBy("avg_rating", ascending=False)
    df_top_series.show(20)
    return df_top_series

def get_actors_with_most_genre_diversity(df_s: Dict[str, DataFrame]):
    basics = df_s["title.basics"]
    principals = df_s["title.principals"]
    names = df_s["name.basics"]

    df_movies = basics.filter((col("genres").isNotNull()) & (col("titleType").isin("movie", "tvMovie")))
    df_actors = principals.filter(col("category").isin("actor", "actress"))
    
    df_joined = df_movies.join(df_actors, "tconst").join(names, df_actors.nconst == names.nconst)
    df_exploded = df_joined.withColumn("genre", explode(split(col("genres"), ",")))
    
    df_genre_count = df_exploded.groupBy("primaryName").agg(countDistinct("genre").alias("genre_diversity")).orderBy("genre_diversity", ascending=False)
    df_genre_count.show(20)
    return df_genre_count

def get_top_rated_movies_by_new_directors(df_s: Dict[str, DataFrame]):
    basics = df_s["title.basics"]
    crew = df_s["title.crew"]
    names = df_s["name.basics"]
    ratings = df_s["title.ratings"]

    df_recent = basics.filter((col("titleType") == "movie") & (col("startYear") >= "2014"))
    df_directors = crew.withColumn("director", explode(split(col("directors"), ",")))

    df_joined = df_recent.join(df_directors, "tconst", "inner")\
                         .join(names, col("director") == names.nconst)\
                         .join(ratings, "tconst")

    df_result = df_joined.select("primaryTitle", "primaryName", "averageRating", "startYear").orderBy("averageRating", ascending=False)
    df_result.show(20)
    return df_result


def get_genre_trends_over_time(df_s: Dict[str, DataFrame]):
    basics = df_s["title.basics"]

    df_filtered = basics.filter((col("startYear").isNotNull()) & (col("genres").isNotNull()) & (col("startYear") != '\\N'))
    df_exploded = df_filtered.withColumn("genre", explode(split(col("genres"), ",")))
    df_by_decade = df_exploded.withColumn("decade", (floor(col("startYear").cast("int") / 10) * 10))

    df_genre_trend = df_by_decade.groupBy("decade", "genre").count().orderBy("decade", "count", ascending=[True, False])
    df_genre_trend.show(20)
    return df_genre_trend


def get_most_versatile_directors(df_s: Dict[str, DataFrame]):
    basics = df_s["title.basics"]
    crew = df_s["title.crew"]
    names = df_s["name.basics"]

    df_movies = basics.filter(col("genres").isNotNull())
    df_directors = crew.withColumn("director", explode(split(col("directors"), ",")))

    df_joined = df_movies.join(df_directors, "tconst", "inner").join(names, col("director") == names.nconst)
    df_exploded = df_joined.withColumn("genre", explode(split(col("genres"), ",")))

    df_genre_counts = df_exploded.groupBy("primaryName").agg(countDistinct("genre").alias("genre_count"))
    df_top_versatile = df_genre_counts.orderBy("genre_count", ascending=False)

    df_top_versatile.show(20)
    return df_top_versatile
