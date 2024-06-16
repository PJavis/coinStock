import json

import pyhdfs
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Crypto Dependency Analysis") \
    .config("spark.mongodb.output.uri", "mongodb://root:admin@mongodb:27017/bigdata.stock2024") \
    .getOrCreate()

# Set up the HDFS client
hdfs = pyhdfs.HdfsClient(hosts="namenode:9870", user_name="hdfs")
directory = '/data'
if not hdfs.exists(directory):
    hdfs.mkdirs(directory)
files = hdfs.listdir(directory)
print("Files in '{}':".format(directory), files)

# Define the schema for the DataFrame
schema = StructType([
    StructField("iso", StringType(), True),
    StructField("name", StringType(), True),
    StructField("current_price", DoubleType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("date_time", StringType(), True)
])


# Function to create a DataFrame from a file's content
def create_dataframe_from_file(file_path):
    try:
        file_content = hdfs.open(file_path).read().decode('utf-8')
        data = json.loads(file_content)
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
        df = df.unionByName(file_df)

# Remove duplicates
df = df.dropDuplicates()

# Filter out Bitcoin data and prepare it as a feature
btc_df = df.filter(df.iso == 'BTC').select("date_time", df.current_price.alias("btc_price"))

# Join BTC data with the main dataframe on date_time
df = df.join(btc_df, on="date_time")

# Assemble features for ML model
assembler = VectorAssembler(inputCols=["btc_price"], outputCol="features")
df = assembler.transform(df)

# Define the model
lr = LinearRegression(featuresCol="features", labelCol="current_price")

# Train the model for each cryptocurrency excluding BTC
results = []
cryptos = df.select("iso").distinct().filter(df.iso != 'BTC').collect()
for crypto in cryptos:
    iso = crypto.iso
    crypto_df = df.filter(df.iso == lit(iso))

    if crypto_df.count() > 0:
        train, test = crypto_df.randomSplit([0.8, 0.2], seed=42)
        lr_model = lr.fit(train)
        predictions = lr_model.transform(test)

        evaluator = RegressionEvaluator(labelCol="current_price", predictionCol="prediction", metricName="r2")
        r2 = evaluator.evaluate(predictions)

        results.append((iso, r2))

# Convert results to DataFrame
results_df = spark.createDataFrame(results, ["iso", "r2_score"])

# Show the results
results_df.show()

# Write the results DataFrame to MongoDB
results_df.write.format("mongo").mode("append").option("database", "bigdata").option("collection",
                                                                                     "crypto_dependency").save()

# Stop SparkSession
spark.stop()
