#importing required packages
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, create_map, lit, from_json
import config as cnf

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Load CSV from GCS") \
    .getOrCreate()

# Set the Google Cloud Storage path to your CSV file
gcs_path = cnf.bucket+"/"+str(cnf.filePaths["movie-data"])

# Read the CSV file into a DataFrame
df = spark.read.csv(gcs_path, header=True, inferSchema=True)

#selecting the required columns into the dataframe and typecasting the columns
#into their respective big query column types
df = df.select("adult","belongs_to_collection","budget")
df_converted=df.withColumn("belongs_to_collection",from_json(col("belongs_to_collection"), cnf.movie_structures["belongs_to_collection"]))

#df_converted=df_converted.select("adult","belongs_to_collection")
#df1=df.select("adult", create_map(lit("id"), col("belongs_to_collection.id"), col("belongs_to_collection.name")).alias("belongs_to_collection"))
# Show the DataFrame
df_converted.show(truncate=False)

# Stop the SparkSession
spark.stop()
