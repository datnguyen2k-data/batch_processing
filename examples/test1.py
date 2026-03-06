from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.types import (StructType, StructField, StringType, IntegerType)
from datetime import datetime
from pyspark.sql.functions import udf

spark = SparkSession.builder.app_name("test1").getOrCreate()

def load_config(spark_context: SparkContext):
    spark_context._jsc.sc().hadoopConfiguration().set("fs.s3a.access.key", "clickhouse")
    spark_context._jsc.sc().hadoopConfiguration().set("fs.s3a.secret.key", "clickhouse")
    spark_context._jsc.sc().hadoopConfiguration().set("fs.defaultFS", "s3a://")
    spark_context._jsc.sc().hadoopConfiguration().set("fs.s3a.endpoint", "s3.us-east-1.amazonaws.com")
    spark_context._jsc.sc().hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    spark_context._jsc.sc().hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "true")
    spark_context._jsc.sc().hadoopConfiguration().set("fs.s3a.connection.maximum", "1")
    spark_context._jsc.sc().hadoopConfiguration().set("fs.s3a.connection.timeout", "1000")
    spark_context._jsc.sc().hadoopConfiguration().set("fs.s3a.connection.establish.timeout", "5000")

load_config(spark.sparkContext)

value_schema_dict = {
    "type": "struct",
    "name": "chando_arvo_value",
    "fields": [
        {"name": "id", "type": "int", "nullable": True, "metadata": {}},
        {"name": "name", "type": "string", "nullable": True, "metadata": {}}
    ]
}

stream_df = spark.readStream.format("kafka")\
              .option("kafka.bootstrap.servers", "localhost:9092")\
              .option("subscribe", "chando_arvo_value")\
              .option("startingOffsets", "earliest")\
              .option("failOnMissingData", "false")\
              .option("mode", "PERMISSIVE")\
              .load()

stream_df.printSchema()
stream_df.show()

chando_df = stream_df.selectExpr("CAST(value AS STRING) as arvo_value").select(
    from_arvo("arvo_value", json.dumps(value_schema_dict)).alias("chando_data").select("data.*")
)

chando_df.writeStream\
    .format("parquet")\
    .outputMode("append")\
    .trigger(processingTime="10 seconds")\
    .option("path", "s3a://")\
    .option("checkpointLocation", "s3a://warehouse-v/k8/checkpoint")\
    .start()\
    .awaitTermination()
