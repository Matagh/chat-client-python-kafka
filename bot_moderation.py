from pyspark.sql import SparkSession
from pyspark.sql.functions import window

spark = SparkSession \
          .builder \
          .appName("BOT_MODERATION") \
          .getOrCreate()

message_received = spark\
      .readStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "localhost:9092") \
      .option("subscribe", "chat_moderation") \
      .option('includeTimestamp', 'true')\
      .load()
#verifie le nombre de message par user en 5 sec
message_treatment = message_received.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
ds = message_treatment.writeStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("checkpointLocation", "chat_moderation_response").start()

ds.awaitTermination()