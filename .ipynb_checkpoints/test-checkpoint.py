#importing required packages
from pyspark.sql import SparkSession
import config as cnf

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Load CSV from GCS") \
    .getOrCreate()

# Set the Google Cloud Storage path to your CSV file
gcs_path = cnf.bucket+"/"+str(cnf.filePaths["movie-data"])

print(gcs_path)

# Read the CSV file into a DataFrame
df = spark.read.csv(gcs_path, header=True, inferSchema=True)

# Show the DataFrame
df.show()

# Stop the SparkSession
spark.stop()
