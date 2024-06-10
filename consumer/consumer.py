import json
from datetime import datetime
from pathlib import Path

import findspark
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

import hdfs
from InfluxDBWriter import InfluxDBWriter
from script.utils import load_environment_variables

path_to_utils = Path(__file__).parent.parent
sys.path.insert(0, str(path_to_utils))
sys.path.append("/app")

load_dotenv()
findspark.init()
env_vars = load_environment_variables()
KAFKA_TOPIC_NAME = env_vars.get("STOCK_PRICE_KAFKA_TOPIC")
KAFKA_BOOTSTRAP_SERVERS = env_vars.get("KAFKA_BROKERS")

scala_version = '2.12'
spark_version = '3.3.3'
packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:2.8.1'
]

if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("KafkaInfluxDBStreaming")
        .master("spark://spark-master:7077")
        .config("spark.jars.packages", ",".join(packages))
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    stockDataframe = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC_NAME) \
        .load()

    stockDataframe = stockDataframe.select(col("value").cast("string").alias("data"))
    inputStream = stockDataframe.selectExpr("CAST(data as STRING)")

    stock_price_schema = StructType([
        StructField("iso", StringType(), True),
        StructField("name", StringType(), True),
        StructField("current_price", DoubleType(), True),
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True)
    ])

    # Parse JSON data and select columns
    stockDataframe = inputStream.select(from_json(col("data"), stock_price_schema).alias("stock_price"))
    expandedDf = stockDataframe.select("stock_price.*")
    influxdb_writer = InfluxDBWriter('primary', 'stock-price-v1')
    # influxdb_writer = InfluxDBWriter(os.environ.get("INFLUXDB_BUCKET"), os.environ.get("INFLUXDB_MEASUREMENT"))
    print("InfluxDB_Init Done")


    def process_batch(batch_df, batch_id):
        for row in batch_df.collect():
            stock_price = row["stock_price"]
            timestamp = stock_price["date_time"]
            tags = {"iso": stock_price["iso"], "name": stock_price["name"]}
            fields = {
                "open": stock_price['open'],
                "high": stock_price['high'],
                "low": stock_price['low'],
                "close": stock_price['close'],
                "current_price": stock_price['current_price']
            }
            influxdb_writer.process(timestamp, tags, fields)

            # Convert timestamp to ISO format
            row["stock_price"]["date_time"] = datetime.strptime(row["stock_price"]["date_time"],
                                                                "%Y-%m-%d %H:%M:%S%z").isoformat()

            # Convert Row to a dictionary
            row_dict = row["stock_price"]
            json_string = json.dumps(row_dict)
            print(json_string)
            print("----------------------")
            hdfs.write_to_hdfs(json_string)
        print(f"Batch processed {batch_id} done!")


    query = stockDataframe \
        .writeStream \
        .foreachBatch(process_batch) \
        .outputMode("append") \
        .start()

    query.awaitTermination()
