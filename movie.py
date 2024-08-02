from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, FloatType, IntegerType, BooleanType
import config as cnf
# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Load Data to BigQuery") \
    .getOrCreate()

# Set BigQuery temporary GCS bucket (replace with your actual temporary bucket name)
spark.conf.set("temporaryGcsBucket", "temp-bucket-dp")

# Replace with your actual Google Cloud Storage path and file name
gcs_path = cnf.bucket + "/" + str(cnf.filePaths["movie-data"])

# Read CSV file into DataFrame
df = spark.read.csv(gcs_path, header=True, inferSchema=True)

# Example: Convert columns to appropriate types
df = df.withColumn("adult", col("adult").cast(BooleanType()))
df = df.withColumn("video", col("video").cast(BooleanType()))
df = df.withColumn("id", col("id").cast(IntegerType()))
df = df.withColumn("budget", col("budget").cast(FloatType()))
df = df.withColumn("revenue", col("revenue").cast(FloatType()))

# Define schemas for nested JSON columns
belongs_to_collection_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])

genres_schema = ArrayType(StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
]))

production_companies_schema = ArrayType(StructType([
    StructField("name", StringType(), True),
    StructField("id", IntegerType(), True)
]))

production_countries_schema = ArrayType(StructType([
    StructField("iso_3166_1", StringType(), True),
    StructField("name", StringType(), True)
]))

# Apply JSON parsing to DataFrame columns
df = df.withColumn("belongs_to_collection", from_json(col("belongs_to_collection"), belongs_to_collection_schema))
df = df.withColumn("genres", from_json(col("genres"), genres_schema))
df = df.withColumn("production_companies", from_json(col("production_companies"), production_companies_schema))
df = df.withColumn("production_countries", from_json(col("production_countries"), production_countries_schema))

# Select columns to write to BigQuery
selected_columns = [
    "adult", "belongs_to_collection", "budget", "genres", "id", "original_language",
    "original_title", "overview", "production_companies",
    "production_countries", "release_date", "revenue", "runtime", "status",
    "tagline", "title", "video", "vote_average", "vote_count"
]

df = df.select(selected_columns)

output_path = cnf.ExtTablePaths["movie_data_raw"] + "parquet-file"
df.write.mode("overwrite").format("parquet").save(output_path)
# df.printSchema()
df.show()


# Stop SparkSession
spark.stop()
