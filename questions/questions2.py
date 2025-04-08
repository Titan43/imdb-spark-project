from typing import Dict
from pyspark.sql import DataFrame
from pyspark.sql.functions import explode, split, col, countDistinct

def get_comedies_after_2010(df_s: Dict[str, DataFrame]):
    df_basics = df_s["title.basics"]
    comedies = df_basics.filter(
        (col("genres").contains("Comedy")) &
        (col("startYear") > 2010) &
        (col("titleType") == "movie")
    ).select(
        col("tconst").alias("movie_id"),
        col("primaryTitle").alias("title"),
        col("startYear").alias("year"),
        col("genres")
    )
    comedies.show()
    return comedies

def get_top10_rated(df_s: Dict[str, DataFrame]):
    df_ratings = df_s["title.ratings"]
    df_basics = df_s["title.basics"]
    top_rated = df_ratings.alias("r") \
        .join(df_basics.alias("b"), col("r.tconst") == col("b.tconst")) \
        .filter((col("r.numVotes") > 100000) & (col("b.titleType") == "movie")) \
        .select(
            col("b.primaryTitle").alias("title"),
            col("r.averageRating").alias("rating"),
            col("r.numVotes").alias("votes")
        ) \
        .orderBy(col("rating").desc()) \
        .limit(10)
    top_rated.show()
    return top_rated

def get_drama_directors(df_s: Dict[str, DataFrame]):
    df_basics = df_s["title.basics"]
    df_crew = df_s["title.crew"]
    df_names = df_s["name.basics"]
    drama_movies = df_basics.filter(col("genres").contains("Drama")) \
        .select(col("tconst").alias("movie_id"), col("primaryTitle").alias("title"))

    drama_with_directors = drama_movies.alias("m") \
        .join(df_crew.alias("c"), col("m.movie_id") == col("c.tconst")) \
        .select(col("m.title"), col("c.directors"))

    directors_exploded = drama_with_directors \
        .withColumn("director_id", explode(split(col("directors"), ","))) \
        .select("title", "director_id")

    drama_directors = directors_exploded.alias("d") \
        .join(df_names.alias("n"), col("d.director_id") == col("n.nconst")) \
        .select(col("d.title"), col("n.primaryName").alias("director_name"))

    drama_directors.show()
    return drama_directors


def get_breaking_bad_episodes(df_s: Dict[str, DataFrame]):
    df_basics = df_s["title.basics"]
    df_episodes = df_s["title.episode"]

    breaking_bad_df = df_basics.filter(
        (col("primaryTitle") == "Breaking Bad") &
        (col("titleType") == "tvSeries")
    )

    breaking_bad_row = breaking_bad_df.select("tconst").first()
    if not breaking_bad_row:
        return None
    breaking_bad_tconst = breaking_bad_row["tconst"]

    episodes = df_episodes.filter(col("parentTconst") == breaking_bad_tconst) \
        .join(df_basics.select("tconst", "primaryTitle"), "tconst") \
        .select(
            col("seasonNumber").cast("int").alias("season"),
            col("episodeNumber").cast("int").alias("episode"),
            col("primaryTitle").alias("episode_title")
        ) \
        .orderBy("season", "episode")

    episodes.show(truncate=False)
    return episodes

def get_actors_with_most_series(df_s: Dict[str, DataFrame]):
    df_basics = df_s["title.basics"]
    df_principals = df_s["title.principals"]
    df_names = df_s["name.basics"]
    series_df = df_basics.filter(col("titleType") == "tvSeries") \
        .select(col("tconst").alias("series_id"))

    series_actors = df_principals.filter(col("category") == "actor") \
        .select(col("nconst"), col("tconst"))

    actors_in_series = series_actors.join(series_df, series_actors["tconst"] == series_df["series_id"]) \
        .groupBy("nconst") \
        .agg(countDistinct("series_id").alias("series_count")) \
        .orderBy(col("series_count").desc()) \
        .limit(10)

    top_series_actors = actors_in_series.join(df_names, "nconst") \
        .select(col("primaryName").alias("actor_name"), col("series_count"))

    top_series_actors.show()
    return top_series_actors


def get_top10_genres_by_film_count(df_s: Dict[str, DataFrame]):
    df_basics = df_s["title.basics"]
    genres_df = df_basics.filter(col("titleType") == "movie") \
        .select(explode(split(col("genres"), ",")).alias("genre"))

    genres_filtered = genres_df.filter(col("genre") != "\\N")

    top_genres = genres_filtered.groupBy("genre") \
        .count() \
        .orderBy(col("count").desc()) \
        .withColumnRenamed("count", "title_count") \
        .limit(10)

    top_genres.show()
    return top_genres