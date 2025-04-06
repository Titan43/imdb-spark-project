from pyspark.sql import SparkSession
from utils.data_loader import DataLoader
from pyspark.sql.window import Window
from pyspark.sql.functions import explode, split, col, avg, year, floor, row_number, countDistinct

spark = SparkSession.builder.appName("DataProcessingApp").getOrCreate()

data_loader = DataLoader(spark, "/app/.cache")

# IMDb Datasets URLs
akas_URL = "https://datasets.imdbws.com/title.akas.tsv.gz"
basics_URL = "https://datasets.imdbws.com/title.basics.tsv.gz"
crew_URL = "https://datasets.imdbws.com/title.crew.tsv.gz"
episode_URL = "https://datasets.imdbws.com/title.episode.tsv.gz"
principals_URL = "https://datasets.imdbws.com/title.principals.tsv.gz"
ratings_URL = "https://datasets.imdbws.com/title.ratings.tsv.gz"
names_URL = "https://datasets.imdbws.com/name.basics.tsv.gz"

# Завантаження
akas_df = data_loader.load_data(akas_URL)
basics_df = data_loader.load_data(basics_URL)
crew_df = data_loader.load_data(crew_URL)
episode_df = data_loader.load_data(episode_URL)
principals_df = data_loader.load_data(principals_URL)
ratings_df = data_loader.load_data(ratings_URL)
names_df = data_loader.load_data(names_URL)



#Список фільмів із жанром "Comedy" після 2010 року
comedies = basics_df.filter(
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

#Топ-10 фільмів за рейтингом з 100k+ голосів
top_rated = ratings_df.alias("r") \
    .join(basics_df.alias("b"), col("r.tconst") == col("b.tconst")) \
    .filter((col("r.numVotes") > 100000) & (col("b.titleType") == "movie")) \
    .select(
        col("b.primaryTitle").alias("title"),
        col("r.averageRating").alias("rating"),
        col("r.numVotes").alias("votes")
    ) \
    .orderBy(col("rating").desc()) \
    .limit(10)

top_rated.show()


#Список режисерів, які зняли фільми в жанрі "Drama"
drama_movies = basics_df.filter(col("genres").contains("Drama")) \
    .select(col("tconst").alias("movie_id"), col("primaryTitle").alias("title"))

drama_with_directors = drama_movies.alias("m") \
    .join(crew_df.alias("c"), col("m.movie_id") == col("c.tconst")) \
    .select(col("m.title"), col("c.directors"))

directors_exploded = drama_with_directors \
    .withColumn("director_id", explode(split(col("directors"), ","))) \
    .select("title", "director_id")

drama_directors = directors_exploded.alias("d") \
    .join(names_df.alias("n"), col("d.director_id") == col("n.nconst")) \
    .select(col("d.title"), col("n.primaryName").alias("director_name"))

drama_directors.show()

#Епізоди серіалу "Breaking Bad"
breaking_bad = basics_df.filter(col("primaryTitle") == "Breaking Bad") \
                        .select("tconst").first()["tconst"]

# Витягуємо епізоди і сортуємо як числа
episodes = episode_df.filter(col("parentTconst") == breaking_bad) \
    .join(basics_df.select("tconst", "primaryTitle"), "tconst") \
    .select(
        col("seasonNumber").cast("int").alias("season"),
        col("episodeNumber").cast("int").alias("episode"),
        col("primaryTitle").alias("episode_title")
    ) \
    .orderBy("season", "episode")

episodes.show()

#Актори, які знялися в найбільшій кількості серіалів
series_df = basics_df.filter(col("titleType") == "tvSeries") \
    .select(col("tconst").alias("series_id"))

# Актори в серіалах
series_actors = principals_df.filter(col("category") == "actor") \
    .select(col("nconst"), col("tconst"))

# Актори, які були в серіалах
actors_in_series = series_actors.join(series_df, series_actors["tconst"] == series_df["series_id"]) \
    .groupBy("nconst") \
    .agg(countDistinct("series_id").alias("series_count")) \
    .orderBy(col("series_count").desc()) \
    .limit(10)

# Додаємо імена
top_series_actors = actors_in_series.join(names_df, "nconst") \
    .select(col("primaryName").alias("actor_name"), col("series_count"))

top_series_actors.show()


#ТОП-10 жанрів за кількістю фільмів
genres_df = basics_df.filter(col("titleType") == "movie") \
    .select(explode(split(col("genres"), ",")).alias("genre"))

# Фільтруємо пропущені жанри
genres_filtered = genres_df.filter(col("genre") != "\\N")

# Підрахунок і сортування
top_genres = genres_filtered.groupBy("genre") \
    .count() \
    .orderBy(col("count").desc()) \
    .withColumnRenamed("count", "title_count") \
    .limit(10)

top_genres.show()