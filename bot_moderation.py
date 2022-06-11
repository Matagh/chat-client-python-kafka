from pyspark import SparkSession

spark = SparkSession \
          .builder \
          .appName("BOT_MODERATION") \
          .getOrCreate()

df = spark\
      .readStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "localhost:9092") \
      .option("subscribe", "chat_moderation") \
      .option("startingOffsets", "earliest") \
      .load()

ds = df
  .writeStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "localhost:9092", "chat_moderation")
  .start()

ds.awaitTermination()