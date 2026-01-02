"""Test file for Kafka streaming (using Pydantic config)."""
from pyspark.sql import SparkSession
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field


class KafkaStreamConfig(BaseSettings):
    """Kafka streaming configuration."""
    kafka_topic: str = Field(default="", alias="KAFKA_TOPIC")
    kafka_bootstrap_servers: str = Field(default="", alias="KAFKA_BOOTSTRAP_SERVERS")
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )


config = KafkaStreamConfig()
spark = SparkSession.builder.appName("SparkKafkaApp").getOrCreate()

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", config.kafka_bootstrap_servers) \
  .option("subscribe", config.kafka_topic) \
  .load()
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")