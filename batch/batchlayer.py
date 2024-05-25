from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pymongo import MongoClient

from operator import add
import sys, os

spark = SparkSession \
    .builder \
    .appName("HDFS to Spark") \
    .master("local[*]") \
    .getOrCreate()


schema = StructType([
  # Define schema fields based on your JSON data types
  StructField("iso", StringType(), True),
  StructField("name", StringType(), True),
  StructField("date_time", StringType(), True),
  StructField("current_price", DoubleType(), True), 
  StructField("open_price", DoubleType(), True),
  StructField("high_price", DoubleType(), True),
  StructField("low_price", DoubleType(), True),
  StructField("close_price", DoubleType(), True),
    ])

# Đường dẫn tới tập dữ liệu trên HDFS
data_path = "hdfs://localhost:9000/user/root/input/data.csv"


try:
  # Đọc dữ liệu JSON từ HDFS
  df_raw = spark.read.schema(schema).csv(data_path)

  df_raw.cache()

  df_raw.show()
  df_raw.printSchema()

except Exception as e:
  # Xử lý lỗi
  print("Lỗi khi đọc dữ liệu:", e)


# mongo_client = MongoClient("mongodb://localhost:27017/") 
# mongo_db = mongo_client["mydata"] 
# mongo_collection = mongo_db["coin"] 


# df_dict = df_raw.rdd.map(lambda x: x.asDict()).collect()

# mongo_collection.insert_many(df_dict)


# Đóng Spark session
spark.stop()

print("Batch processing completed and results saved.")
