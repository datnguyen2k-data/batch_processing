from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os

def get_spark_clickhouse(app_name="SparkClickHouseApp"):
    load_dotenv()

    spark = SparkSession.builder.appName(app_name).getOrCreate()

    spark.conf.set("spark.sql.catalog.clickhouse", "com.clickhouse.spark.ClickHouseCatalog")
    spark.conf.set("spark.sql.catalog.clickhouse.host", os.getenv("CLICKHOUSE_HOST"))
    spark.conf.set("spark.sql.catalog.clickhouse.protocol", "http")
    spark.conf.set("spark.sql.catalog.clickhouse.http_port", os.getenv("CLICKHOUSE_PORT"))
    spark.conf.set("spark.sql.catalog.clickhouse.user", os.getenv("CLICKHOUSE_USER"))
    spark.conf.set("spark.sql.catalog.clickhouse.password", os.getenv("CLICKHOUSE_PASSWORD"))
    spark.conf.set("spark.sql.catalog.clickhouse.database", os.getenv("CLICKHOUSE_DB"))

    return spark

def kafka_connector(app_name="SparkKafkaApp"):
    load_dotenv()

    spark = SparkSession.builder.appName(app_name).getOrCreate()

    spark.conf.set("spark.sql.catalog.kafka", "org.apache.spark.sql.kafka.v2.KafkaCatalog")
    spark.conf.set("spark.sql.catalog.kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP_SERVERS"))
    spark.conf.set("spark.sql.catalog.kafka.security.protocol", "SASL_PLAINTEXT")
    spark.conf.set("spark.sql.catalog.kafka.sasl.jaas.config", os.getenv("KAFKA_JAAS_CONFIG"))
    spark.conf.set("spark.sql.catalog.kafka.sasl.mechanisms", os.getenv("KAFKA_SASL_MECHANISMS"))
    spark.conf.set("spark.sql.catalog.kafka.ssl.endpoint.identification.algorithm", "https")
    spark.conf.set("spark.sql.catalog.kafka.ssl.truststore.location", os.getenv("KAFKA_SSL_TRUSTSTORE_LOCATION"))
    spark.conf.set("spark.sql.catalog.kafka.ssl.truststore.password", os.getenv("KAFKA_SSL_TRUSTSTORE_PASSWORD"))
    spark.conf.set("spark.sql.catalog.kafka.ssl.truststore.type", os.getenv("KAFKA_SSL_TRUSTSTORE_TYPE"))
    spark.conf.set("spark.sql.catalog.kafka.ssl.keystore.location", os.getenv("KAFKA_SSL_KEYSTORE_LOCATION"))
    spark.conf.set("spark.sql.catalog.kafka.ssl.keystore.password", os.getenv("KAFKA_SSL_KEYSTORE_PASSWORD"))
    spark.conf.set("spark.sql.catalog.kafka.ssl.keystore.type", os.getenv("KAFKA_SSL_KEYSTORE_TYPE"))
    spark.conf.set("spark.sql.catalog.kafka.ssl.key.password", os.getenv("KAFKA_SSL_KEY_PASSWORD"))
    return spark

