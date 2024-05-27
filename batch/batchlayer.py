from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import *

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

    df_raw.printSchema()
    df_raw.show()

except Exception as e:
    # Xử lý lỗi
    print("Lỗi khi đọc dữ liệu:", e)


def batch_process_data():
    df_with_avg_price = df.withColumn(
        "average_price", (f.col("high_price") + f.col("low_price")) / 2
    )
    return df_with_avg_price


# Process the data
df_processed = batch_process_data(df_raw)

mongo_uri = "mongodb://localhost:27017/myDB"
df_processed.write.format("com.mongodb.spark.sql.mongo") \
    .option("uri", mongo_uri) \
    .option("collection", "coin") \
    .save()

mongo_uri = "mongodb://localhost:27017/myDB"
df = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
    .option("uri", mongo_uri) \
    .option("collection", "coin") \
    .load()

df.show()

# Đóng Spark session
spark.stop()

print("Batch processing completed and results saved.")
