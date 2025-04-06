from pyspark.sql import SparkSession
from questions.df_preload import preload_dfs
from questions import get_question_results

def main():
    spark = SparkSession.builder.appName("DataProcessingApp").getOrCreate()
    df_s = preload_dfs(spark, 
                    ["https://datasets.imdbws.com/title.basics.tsv.gz",
                        "https://datasets.imdbws.com/title.crew.tsv.gz",
                        "https://datasets.imdbws.com/name.basics.tsv.gz"])

    get_question_results.run(df_s)

if __name__ == "__main__":
    main()