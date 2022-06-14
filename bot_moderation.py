
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import window


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
      
#Check DataFrame structure
f= open("/Users/louise/Desktop/FORMATION/Alternance/spark/chat-client-python-kafka/test.txt", "a")
f.write(str(message_received))
f.close()

#send back to a kafka topic in new dataFrame structured with username as value
test_data = message_received.withColumn("value",col("key").cast("string")).withColumn("timestamp",col("timestamp"))
print(type(message_received))
print(type(test_data))
#print(test_data.rdd)
#test_window = test_data.rolling(10)
test_window = test_data.groupby( window(col("timestamp"),"10 seconds"))
ds = test_window.writeStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("topic", "chat_moderation_response").option("checkpointLocation", "/Users/louise/Desktop/FORMATION/Alternance/spark/chat-client-python-kafka/tmp").start()
ds.awaitTermination()