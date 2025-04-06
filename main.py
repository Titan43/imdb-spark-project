from pyspark.sql import SparkSession
from utils.data_loader import DataLoader 
from utils.data_writer import DataWriter 

spark = SparkSession.builder.appName("DataProcessingApp").getOrCreate()

CACHE_PATH = "/app/.cache"

data_loader = DataLoader(spark, CACHE_PATH)
data_writer = DataWriter(CACHE_PATH)

url = "https://datasets.imdbws.com/name.basics.tsv.gz"
df = data_loader.load_data(url)

if df is not None:
    df.show()
    data_writer.save_as_csv(df, "test")
else:
    print("Failed to load data.")
