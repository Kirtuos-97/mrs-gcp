from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, FloatType, IntegerType, BooleanType
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
df = spark.read.csv(gcs_path, header=True, inferSchema=True)

# Print schema to verify column names and types
df.printSchema()

# Example: Convert columns to appropriate types

df = df.withColumn("id", col("id").cast(IntegerType()))


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
df = df.withColumn("cast", from_json(col("cast"), cast_schema))
df = df.withColumn("crew", from_json(col("crew"), crew_schema))


# Select columns to write to BigQuery
selected_columns = [
    "cast", "id", "crew"
]

df = df.select(selected_columns)

# Define the output path for Parquet file
output_path = cnf.ExtTablePaths["credits_data_raw"] + "parquet-file"

# Write DataFrame to Parquet format
df.write.mode("overwrite").format("parquet").save(output_path)

# Print schema and show some data to verify
df.printSchema()
df.show()

# Stop SparkSession
spark.stop()
