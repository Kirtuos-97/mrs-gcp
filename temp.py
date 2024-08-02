from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType
import config as cnf

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Load Data to BigQuery") \
    .getOrCreate()

# Set BigQuery temporary GCS bucket
spark.conf.set("temporaryGcsBucket", "temp-bucket-dp")

# Replace with your actual Google Cloud Storage path and file name
gcs_path = cnf.bucket + "/" + str(cnf.filePaths["credits-data"])

# Read CSV file into DataFrame
try:
    df = spark.read.csv(gcs_path, header=True, inferSchema=True)
    print("CSV file loaded successfully.")
except Exception as e:
    print(f"Error loading CSV file: {e}")

# Print schema to verify column names and types
df.printSchema()

# Check initial data
df.show(5, truncate=False)

# Example: Convert columns to appropriate types
try:
    df = df.withColumn("id", col("id").cast(IntegerType()))
    print("Column types cast successfully.")
except Exception as e:
    print(f"Error casting column types: {e}")

# Define schemas for nested JSON columns (assuming these are correct and match your data)
cast_schema = ArrayType(StructType([
    StructField("cast_id", IntegerType(), True),
    StructField("character", StringType(), True),
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
]))
crew_schema = ArrayType(StructType([
    StructField("department", StringType(), True),
    StructField("id", IntegerType(), True),
    StructField("job", StringType(), True),
    StructField("name", StringType(), True)
]))

# Apply JSON parsing to DataFrame columns
try:
    df = df.withColumn("cast", from_json(col("cast"), cast_schema))
    df = df.withColumn("crew", from_json(col("crew"), crew_schema))
    print("JSON parsing applied successfully.")
except Exception as e:
    print(f"Error parsing JSON columns: {e}")

# Select columns to write to BigQuery
selected_columns = [
    "cast", "id", "crew"
]

df = df.select(selected_columns)

# Define the output path for Parquet file
output_path = cnf.ExtTablePaths["credits_data_raw"] + "parquet-file"

# Write DataFrame to Parquet format
try:
    df.write.mode("overwrite").format("parquet").save(output_path)
    print("DataFrame written to Parquet format successfully.")
except Exception as e:
    print(f"Error writing DataFrame to Parquet: {e}")

# Print schema and show some data to verify
df.printSchema()
df.show(5, truncate=False)

# Stop SparkSession
spark.stop()
