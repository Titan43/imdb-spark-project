from pyspark.sql import SparkSession
from questions.df_preload import preload_dfs

spark = SparkSession.builder.appName("DataProcessingApp").getOrCreate()
df_s = preload_dfs(spark, 
                   ["https://datasets.imdbws.com/title.basics.tsv.gz",
                    "https://datasets.imdbws.com/title.crew.tsv.gz",
                    "https://datasets.imdbws.com/name.basics.tsv.gz"])