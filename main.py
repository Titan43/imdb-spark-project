from pyspark.sql import SparkSession
from utils.data_loader import DataLoader  
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import sum as sum_, when
from pyspark.sql.functions import explode, split, col, avg, year, floor, row_number, count, dense_rank, lag


spark = SparkSession.builder.appName("DataProcessingApp").getOrCreate()
data_loader = DataLoader(spark, "/app/.cache")

url_basics = "https://datasets.imdbws.com/title.basics.tsv.gz"
url_crew = "https://datasets.imdbws.com/title.crew.tsv.gz"
url_names = "https://datasets.imdbws.com/name.basics.tsv.gz"

df_basics = data_loader.load_data(url_basics)
df_crew = data_loader.load_data(url_crew)
df_names  = data_loader.load_data(url_names)


# top 20 genres by average runtime
df_filtered = df_basics.filter(df_basics.runtimeMinutes.isNotNull() & df_basics.genres.isNotNull())
df_exploded = df_filtered.withColumn("genre", explode(split(col("genres"), ",")))
avg_runtime_by_genre = df_exploded.groupBy("genre").agg(avg("runtimeMinutes").alias("avg_runtime")).orderBy("avg_runtime", ascending=False)
avg_runtime_by_genre.show(20)


# top 20 genres dominating long-running series
df_tv_series = df_basics.filter((df_basics.titleType == 'tvSeries') & (df_basics.endYear != '\\N'))
df_tv_series_with_duration = df_tv_series.withColumn(
    "duration", (col("endYear").cast("int") - col("startYear").cast("int"))
)
df_long_running_series = df_tv_series_with_duration.filter(col("duration") > 5)
long_running_series_count = df_long_running_series.count()
print(f"Number of TV series that have run for more than 5 years: {long_running_series_count}")
df_exploded = df_long_running_series.withColumn("genre", explode(split(col("genres"), ",")))
genre_count = df_exploded.groupBy("genre").count().orderBy("count", ascending=False)
genre_count.show(20)


# top 20 years with the most releases
df_filtered = df_basics.filter((df_basics.startYear != '\\N') & (df_basics.startYear.cast("int").isNotNull()))
df_with_decade = df_filtered.withColumn(
    "decade", floor(col("startYear").cast("int") / 10) * 10
)
df_decade_trends = df_with_decade.groupBy("decade").count().orderBy("decade")
df_decade_trends.show()
df_yearly_trends = df_with_decade.groupBy("startYear").count().orderBy("startYear")
df_yearly_trends.show(20)


# top 20 directors with the most movies directed
df_filtered_basics = df_basics.filter(
    (df_basics.titleType.isin("movie", "tvSeries")) & 
    (df_basics.startYear.cast("int") >= 2000) & 
    (df_basics.startYear.cast("int") <= 2020)
)
df_crew_with_directors = df_crew.withColumn("directors_array", split(col("directors"), ","))
df_crew_exploded = df_crew_with_directors.withColumn("director", explode(col("directors_array")))
df_joined = df_filtered_basics.join(
    df_crew_exploded, on="tconst", how="inner"
)
df_director_names = df_joined.join(
    df_names, df_joined.director == df_names.nconst, how="inner"
)
from pyspark.sql import functions as F
df_director_count = df_director_names.groupBy("primaryName").agg(F.count("tconst").alias("num_movies"))
window_spec = Window.orderBy(col("num_movies").desc())
df_director_rank = df_director_count.withColumn("rank", row_number().over(window_spec))
df_director_rank.show(20)


# most common genres for movies directed by the top 20 directors
df_filtered_basics = df_basics.filter(
    (df_basics.titleType.isin("movie", "tvSeries")) & 
    (df_basics.startYear.cast("int") >= 2000) & 
    (df_basics.startYear.cast("int") <= 2020)
)
df_crew_with_directors = df_crew.withColumn("directors_array", split(col("directors"), ","))
df_crew_exploded = df_crew_with_directors.withColumn("director", explode(col("directors_array")))
df_joined = df_filtered_basics.join(
    df_crew_exploded, on="tconst", how="inner"
)
df_director_names = df_joined.join(
    df_names, df_joined.director == df_names.nconst, how="inner"
)
df_director_count = df_director_names.groupBy("primaryName").agg(F.count("tconst").alias("num_movies"))
window_spec = Window.orderBy(col("num_movies").desc())
df_director_rank = df_director_count.withColumn("rank", row_number().over(window_spec))
top_20_directors = df_director_rank.filter(col("rank") <= 20)
top_20_directors_renamed = top_20_directors.withColumnRenamed("primaryName", "top_director_name")
df_top_directors_movies = df_director_names.join(top_20_directors_renamed, df_director_names.primaryName == top_20_directors_renamed.top_director_name)
df_with_genres = df_top_directors_movies.withColumn("genre", explode(split(col("genres"), ",")))
df_genre_count = df_with_genres.groupBy("top_director_name", "genre").agg(count("tconst").alias("genre_count"))
df_genre_count.show(20)

# top 20 most consistent directors
df_movies_only = df_basics.filter(col("titleType") == "movie").filter(col("startYear").isNotNull())
df_crew_directors = df_crew.withColumn("director", explode(split(col("directors"), ",")))
df_movie_directors = df_movies_only.join(df_crew_directors, on="tconst", how="inner")
df_movie_directors_named = df_movie_directors.join(df_names, df_movie_directors.director == df_names.nconst)
df_movie_directors_named = df_movie_directors_named.withColumn("startYear", col("startYear").cast("int"))
year_window = Window.partitionBy("primaryName").orderBy("startYear")
df_with_lag = df_movie_directors_named.withColumn("prev_year", lag("startYear").over(year_window))
df_with_consistency = df_with_lag.withColumn(
    "gap",
    (col("startYear") - col("prev_year")) > 1
)
group_window = Window.partitionBy("primaryName").orderBy("startYear")
df_with_consistency = df_with_consistency.withColumn(
    "group_id",
    sum_(when(col("gap") | col("prev_year").isNull(), 1).otherwise(0)).over(group_window)
)
df_streaks = df_with_consistency.groupBy("primaryName", "group_id").agg(
    F.countDistinct("startYear").alias("consecutive_years")
)
streak_window = Window.partitionBy("primaryName").orderBy(F.col("consecutive_years").desc())
df_longest_streak = df_streaks.withColumn("rank", row_number().over(streak_window)).filter(col("rank") == 1)
df_top_streaks = df_longest_streak.orderBy(col("consecutive_years").desc())
df_top_streaks.show(20, truncate=False)
