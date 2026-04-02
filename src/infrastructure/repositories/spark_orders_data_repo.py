from pyspark.sql import SparkSession, DataFrame
from src.domain.orders.data_port import IOrdersDataPort

class SparkOrdersDataRepo(IOrdersDataPort):
    """Implementation of IOrdersDataPort utilizing PySpark Catalog to talk with ClickHouse."""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        
    def read_orders(self) -> DataFrame:
        """Fetch the physical orders table from ClickHouse schema."""
        # Using Spark 3.x Catalog V2
        # Setup: <catalog>.<database>.<table>
        return self.spark.table("clickhouse.clickhouse.orders")
        
    def read_order_lines(self) -> DataFrame:
        """Fetch the physical order_line table from ClickHouse schema."""
        return self.spark.table("clickhouse.clickhouse.order_line")
        
    def write_fact_order_line(self, df: DataFrame) -> None:
        """Persist the joined output to ClickHouse."""
        # Using append mode. If partitioned or sorted, ClickHouse natively handles it 
        # based on how fact_order_line was created.
        df.writeTo("clickhouse.clickhouse.fact_order_line").append()
