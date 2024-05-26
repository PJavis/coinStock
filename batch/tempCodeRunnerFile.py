mongo_uri = "mongodb://localhost:27017/myDB"
# df_processed.write.format("com.mongodb.spark.sql.mongo") \
# .option("uri", mongo_uri) \
# .option("collection", "coin") \
# .save()