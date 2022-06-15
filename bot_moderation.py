
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import window

#Crée une session spark
spark = SparkSession \
          .builder \
          .appName("BOT_MODERATION") \
          .getOrCreate()

#lie spark au kafka stream sur le topic associé à la modération
message_received = spark\
      .readStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "localhost:9092") \
      .option("subscribe", "chat_moderation") \
      .option('includeTimestamp', 'true')\
      .load()


#transforme le dataFrame pour ne garder que la clé (username) de la paire reçue
data_transformed = message_received.withColumn("value",col("key").cast("string"))

#on renvoie la data à kafka dans un topic dédié à la modération
ds = data_transformed.writeStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092")\
                  .option("topic", "chat_moderation_response") \
                  .option("checkpointLocation", "/Users/louise/Desktop/FORMATION/Alternance/spark/chat-client-python-kafka/tmp")\
                  .start()

ds.awaitTermination()