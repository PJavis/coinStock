import json

import pyhdfs
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Stock Analysis") \
    .config("spark.mongodb.output.uri", "mongodb://root:admin@mongodb:27017/bigdata.stock2024") \
    .getOrCreate()

# Setup the HDFS client
hdfs = pyhdfs.HdfsClient(hosts="namenode:9870", user_name="hdfs")
directory = '/data'
files = hdfs.listdir(directory)
print("Files in '{}':".format(directory), files)

# Define the schema for the DataFrame
schema = StructType([
    StructField("iso", StringType(), True),
    StructField("name", StringType(), True),
    StructField("date_time", StringType(), True),
    StructField("current_price", DoubleType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True)
])


# Function to create a DataFrame from a file's content
def create_dataframe_from_file(file_path):
    try:
        # Read the file content
        file_content = hdfs.open(file_path).read().decode('utf-8')
        # Convert JSON content to Python dictionary
        data = json.loads(file_content)
        # Create a DataFrame using the defined schema
        return spark.createDataFrame([data], schema)
    except Exception as e:
        print("Failed to read '{}': {}".format(file_path, e))
        return None


# Create an empty DataFrame with the specified schema
df = spark.createDataFrame([], schema)

# Iterate over files and create DataFrame
for file in files:
    file_path = "{}/{}".format(directory, file)
    file_df = create_dataframe_from_file(file_path)
    if file_df:
        # Convert "date_time" column to TimestampType
        file_df = file_df.withColumn("date_time", to_timestamp(file_df["date_time"], "yyyy-MM-dd HH:mm:ssZ"))
        df = df.unionByName(file_df)

# Remove duplicates
df = df.dropDuplicates()

# Basic Statistics for each stock
basic_stats = df.groupBy("iso").agg(
    F.mean("open").alias("avg_open"),
    F.mean("high").alias("avg_high"),
    F.mean("low").alias("avg_low"),
    F.mean("close").alias("avg_close"),
    F.stddev("close").alias("std_dev_close"),
    F.max("high").alias("historical_high"),
    F.min("low").alias("historical_low")
)

# Show basic statistics
basic_stats.show()

# Write basic_stats DataFrame to MongoDB
basic_stats.write.format("mongo").mode("append").save()

# Stop SparkSession
spark.stop()
