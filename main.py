from pyspark.sql import SparkSession
from utils.data_loader import DataLoader  

spark = SparkSession.builder.appName("DataProcessingApp").getOrCreate()

data_loader = DataLoader(spark, "/app/.cache")

url = "https://datasets.imdbws.com/name.basics.tsv.gz"
df = data_loader.load_data(url)

if df is not None:
    df.show()
else:
    print("Failed to load data.")
