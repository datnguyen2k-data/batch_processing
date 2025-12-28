from pyspark.sql import SparkSession, Row
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, DoubleType, StringType,
    DateType, TimestampType
)
from datetime import date, datetime
from pyspark.sql.functions import udf
from connector.connector import get_spark_clickhouse

spark = get_spark_clickhouse()

def normalize_string(text: str) -> str:
    return text.lower().strip()

normalize_string_udf = udf(normalize_string, StringType())
spark.udf.register("normalize_string", normalize_string_udf)

class DateDefault(date):
    @property
    def ymd(self):
        """Trả về định dạng YYYY-MM-DD"""
        return self.strftime("%Y-%m-%d")
    
    @property
    def weekday_name(self):
        """Trả về thứ trong tuần (Tiếng Anh)"""
        days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        return days[self.weekday()]
    
    @property
    def week_of_year(self):
        """Trả về số tuần trong năm"""
        return int(self.strftime("%U"))

date_default_udf = udf(DateDefault, DateType())
spark.udf.register("date_default", date_default_udf)


order_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer", StringType(), True),
    StructField("order_date", DateType(), True),
    StructField("order_status", StringType(), True),
])
order_data = [
    Row(order_id=1, customer="Alice", order_date=date(2024,1,1), order_status = 'sale'),
    Row(order_id=2, customer="Bob",   order_date=date(2024,1,2), order_status = 'sale'),
    Row(order_id=3, customer="Chris", order_date=date(2024,1,3), order_status = 'sale'),
]

df_order = spark.createDataFrame(order_data, schema=order_schema)
orderline_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("item", StringType(), True),
    StructField("qty", IntegerType(), True),
    StructField("price", DoubleType(), True),
])

orderline_data = [
    Row(order_id=1, item="A01", qty=2, price=100.0),
    Row(order_id=1, item="B02", qty=1, price=50.0),
    Row(order_id=2, item="C03", qty=3, price=80.0),
    Row(order_id=3, item="A01", qty=1, price=100.0),
    Row(order_id=3, item="D04", qty=2, price=60.0),
]

df_orderline = spark.createDataFrame(orderline_data, schema=orderline_schema)


# df_join = df_order.join(df_orderline, df_order.order_id == df_orderline.order_id, "inner")
# df_join.show()
# df_join.printSchema()

df_order.createOrReplaceTempView("orders")
df_orderline.createOrReplaceTempView("orderlines")


query = """
WITH order_amounts AS (
    SELECT
        l.order_id,
        SUM(l.qty * l.price) AS total_amount
    FROM orderlines l
    GROUP BY l.order_id
),
order_customer AS (
    SELECT
        o.order_id,
        o.customer,
        o.order_date,
        o.order_status
    FROM orders o
)
SELECT
    oc.order_id,
    oc.customer,
    oc.order_date,
    oc.order_status,
    oa.total_amount
FROM order_customer oc
JOIN order_amounts oa
    ON oc.order_id = oa.order_id
ORDER BY oc.order_id
"""

df_join = spark.sql(query)
df_join.show()
df_join.printSchema()

df_join_norn = df_join.withColumn("customer", normalize_string_udf("customer"))
df_join_norn.show() 
df_join_norn.printSchema()

df_join_norn.writeTo("clickhouse.analytics.orders").append()

# df_join.writeTo("clickhouse.analytics.spark_test").append()

# GHI VÀO CLICKHOUSE
# df.writeTo("clickhouse.analytics.spark_test").append()

# spark.stop()
# data = [Row(id=11, name="John"), Row(id=12, name="Doe")]
# df_test = spark.createDataFrame(data)

# # Write DataFrame to ClickHouse
# df_test.writeTo("clickhouse.default.example_table").append()

