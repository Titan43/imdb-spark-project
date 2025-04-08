from typing import Dict
from pyspark.sql.window import Window
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.functions import sum as sum_, when
from pyspark.sql.functions import explode, split, col, avg, year, floor, row_number, count, dense_rank, lag, lit, regexp_replace, lower

# Десятиріччя з найбільшою кількістю анімаційних фільмів
def get_animation_count_by_decade(df_s):
    df_basics = df_s["title.basics"]
    df_filtered = df_basics.filter(df_basics.genres.contains("Animation") & df_basics.startYear.isNotNull() & df_basics.startYear.cast("int").between(1900, 2029))
    df_decade = df_filtered.withColumn("decade", (col("startYear").cast("int") / 10).cast("int") * 10)
    decade_counts = df_decade.groupBy("decade").count().orderBy("decade")
    decade_counts.show()
    return decade_counts

#Актори, які нині найдовше в індустрії
def get_longest_active_people(df_s):
    df_name = df_s["name.basics"]
    df_filtered = df_name.filter(
        ((col("deathYear").isNull()) | (col("deathYear") == "\\N")) & 
        (col("birthYear").isNotNull()) & (col("birthYear") != "\\N") & 
        (col("knownForTitles").isNotNull()) & (col("knownForTitles") != "\\N")
    )
    df_casted = df_filtered.withColumn("birthYearInt", col("birthYear").cast("int"))
    df_recent = df_casted.filter(col("birthYearInt") > 1930)
    df_span = df_recent.withColumn("active_span", lit(2025) - col("birthYearInt"))
    longest_active = df_span.select("primaryName", "birthYear", "active_span").orderBy("active_span", ascending=False)
    longest_active.show(20)
    return longest_active

# Найкраща анімація за роками
def get_top_animated_title_by_year(df_s):
    df_basics = df_s["title.basics"]
    df_ratings = df_s["title.ratings"]
    df_joined = df_basics.join(df_ratings, "tconst")
    df_filtered = df_joined.filter(df_joined.genres.contains("Animation") & df_joined.startYear.isNotNull() & df_joined.averageRating.isNotNull())
    df_filtered = df_filtered.withColumn("year", col("startYear").cast("int")).filter((col("year") >= 1900) & (col("year") <= 2025))
    window_spec = Window.partitionBy("year").orderBy(col("averageRating").desc())
    df_ranked = df_filtered.withColumn("rank", row_number().over(window_spec)).filter(col("rank") == 1)
    df_ranked.select("year", "primaryTitle", "averageRating", "genres").orderBy("year").show(50)
    return df_ranked

#Найкращі режисери анімації з принаймні трьома роботами
def get_top_animation_directors(df_s):
    df_basics = df_s["title.basics"]
    df_ratings = df_s["title.ratings"]
    df_crew = df_s["title.crew"]
    df_names = df_s["name.basics"]
    df_joined = df_basics.join(df_ratings, "tconst").join(df_crew, "tconst")
    df_filtered = df_joined.filter(df_joined.genres.contains("Animation") & df_joined.directors.isNotNull())
    df_directors = df_filtered.withColumn("director_id", explode(split(col("directors"), ",")))
    df_avg_rating = df_directors.groupBy("director_id").agg(count("*").alias("num_animated_titles"),avg("averageRating").alias("avg_rating")).filter(col("num_animated_titles") >= 3)
    df_with_names = df_avg_rating.join(df_names, df_avg_rating.director_id == df_names.nconst)
    result = df_with_names.select("primaryName", "num_animated_titles", "avg_rating").orderBy("avg_rating", ascending=False)
    result.show(20)
    return result

# Порівняння рейтингів популярних фільмів і їх сиквелів
def compare_originals_and_sequels(df_s):
    df_basics = df_s["title.basics"]
    df_ratings = df_s["title.ratings"]
    df_joined = df_basics.join(df_ratings, "tconst")
    df_sequels = df_joined.filter((col("primaryTitle").rlike(".* [2-9]$|.* [IVX]+$")) & col("startYear").isNotNull() & col("averageRating").isNotNull()
    ).withColumn("baseTitle", regexp_replace(col("primaryTitle"), " [2-9]$| [IVX]+$", "")) \
     .select(
        col("baseTitle"),
        col("primaryTitle").alias("sequelTitle"),
        col("averageRating").alias("sequelRating")
    )
    df_originals = df_joined.filter((~col("primaryTitle").rlike(".* [2-9]$|.* [IVX]+$")) & col("startYear").isNotNull() & col("averageRating").isNotNull()
    ).withColumn("baseTitle", col("primaryTitle")) \
     .select(
        col("baseTitle"),
        col("primaryTitle").alias("originalTitle"),
        col("averageRating").alias("originalRating")
    )
    df_comparison = df_sequels.join(df_originals, on="baseTitle", how="inner")
    df_result = df_comparison.select(
        "baseTitle",
        "originalTitle",
        "sequelTitle",
        "originalRating",
        "sequelRating",
        (col("sequelRating") - col("originalRating")).alias("rating_diff")
    ).orderBy("rating_diff")
    df_result.show(20, truncate=False)
    return df_result

# Найуспішніші франшизи за сумою рейтингів фільмів у них
def get_top_franchises_by_rating_count(df_s):
    df_basics = df_s["title.basics"]
    df_ratings = df_s["title.ratings"]
    df_joined = df_basics.join(df_ratings, "tconst")
    df_filtered = df_joined.filter((df_joined.titleType == "movie") & df_joined.primaryTitle.isNotNull() & df_joined.numVotes.isNotNull())
    df_base = df_filtered.withColumn("baseTitle", regexp_replace(lower(col("primaryTitle")), r"[:\-]?\s?(part|episode)?\s?(ii+|[2-9])$", ""))
    df_franchise = df_base.groupBy("baseTitle").agg(count("*").alias("num_titles"), sum_("numVotes").alias("total_votes")
    ).filter(col("num_titles") > 1).orderBy("total_votes", ascending=False)
    df_franchise.show(30, truncate=False)
    return df_franchise