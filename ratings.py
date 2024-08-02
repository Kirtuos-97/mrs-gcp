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
gcs_path = cnf.bucket + "/" + str(cnf.filePaths["rating-data"])

# Read CSV file into DataFrame
df = spark.read.csv(gcs_path, header=True, inferSchema=True)

# Example: Convert columns to appropriate types
df = df.withColumn("movieId", col("movieId").cast(IntegerType()))
df = df.withColumn("rating", col("rating").cast(FloatType()))
df = df.withColumn("userid", col("userid").cast(IntegerType()))
df = df.withColumn("timestamp", col("timestamp").cast(IntegerType()))




# Select columns to write to BigQuery
selected_columns = [
    "movieId", "rating", "userid", "timestamp"
]

df = df.select(selected_columns)

output_path = cnf.ExtTablePaths["rating_data_raw"] + "parquet-file"
df.write.mode("overwrite").format("parquet").save(output_path)
# df.printSchema()
df.show()


# Stop SparkSession
spark.stop()
