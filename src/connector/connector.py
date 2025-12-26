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
    spark.conf.set("spark.sql.catalog.clickhouse.password", os.getenv("CLICKHOUSE_PASS"))
    spark.conf.set("spark.sql.catalog.clickhouse.database", os.getenv("CLICKHOUSE_DB"))

    return spark

