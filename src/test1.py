from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os

load_dotenv()

spark = SparkSession.builder.appName("SparkKafkaApp").getOrCreate()

kafka_topic = os.getenv("KAFKA_TOPIC")
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP_SERVERS")) \
  .option("subscribe", kafka_topic) \
  .load()
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")