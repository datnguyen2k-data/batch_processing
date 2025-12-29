"""
Examples demonstrating different loading strategies:
1. Delete + Insert
2. Merge/Upsert
3. SCD Type 2
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import current_timestamp, lit
from src.infrastructure.connectors.spark_connector import SparkConnector
from src.application.batch_processor import BatchProcessor


def create_sample_data(spark: SparkSession):
    """Create sample DataFrames for examples."""
    # Sample customer data
    customer_schema = StructType([
        StructField("customer_id", IntegerType(), False),
        StructField("customer_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("city", StringType(), True),
        StructField("status", StringType(), True),
    ])
    
    customer_data = [
        (1, "Nguyễn Văn A", "nguyenvana@example.com", "Hà Nội", "active"),
        (2, "Trần Thị B", "tranthib@example.com", "Hồ Chí Minh", "active"),
        (3, "Lê Văn C", "levanc@example.com", "Đà Nẵng", "inactive"),
    ]
    
    customer_df = spark.createDataFrame(customer_data, customer_schema)
    
    # Sample order data
    order_schema = StructType([
        StructField("order_id", IntegerType(), False),
        StructField("customer_id", IntegerType(), False),
        StructField("product_name", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("amount", IntegerType(), True),
    ])
    
    order_data = [
        (101, 1, "Laptop", 1, 15000000),
        (102, 1, "Mouse", 2, 500000),
        (103, 2, "Keyboard", 1, 1200000),
    ]
    
    order_df = spark.createDataFrame(order_data, order_schema)
    
    return customer_df, order_df


def example_delete_insert(spark: SparkSession, processor: BatchProcessor):
    """Example 1: Delete + Insert strategy."""
    print("\n=== Example 1: Delete + Insert Strategy ===")
    
    customer_df, _ = create_sample_data(spark)
    
    # Load initial data
    processor.load_data(
        source_df=customer_df,
        target_table="customers",
        strategy="delete_insert",
        target_catalog="iceberg"
    )
    print("✓ Initial data loaded")
    
    # Create updated data
    updated_customer_data = [
        (1, "Nguyễn Văn A Updated", "nguyenvana@example.com", "Hà Nội", "active"),
        (4, "Phạm Thị D", "phamthid@example.com", "Cần Thơ", "active"),
    ]
    updated_df = spark.createDataFrame(updated_customer_data, customer_df.schema)
    
    # Delete and insert with filter condition
    processor.load_data(
        source_df=updated_df,
        target_table="customers",
        strategy="delete_insert",
        target_catalog="iceberg",
        filter_condition="customer_id IN (1, 4)"
    )
    print("✓ Data refreshed with delete + insert")
    
    # Verify
    result = spark.table("iceberg.customers")
    print(f"Total records: {result.count()}")
    result.show()


def example_merge_upsert(spark: SparkSession, processor: BatchProcessor):
    """Example 2: Merge/Upsert strategy."""
    print("\n=== Example 2: Merge/Upsert Strategy ===")
    
    customer_df, _ = create_sample_data(spark)
    
    # Load initial data
    processor.load_data(
        source_df=customer_df,
        target_table="customers_merge",
        strategy="merge_upsert",
        target_catalog="iceberg",
        merge_keys=["customer_id"]
    )
    print("✓ Initial data loaded")
    
    # Create incremental updates
    incremental_data = [
        (1, "Nguyễn Văn A Updated", "nguyenvana.new@example.com", "Hà Nội", "active"),  # Update
        (4, "Phạm Thị D", "phamthid@example.com", "Cần Thơ", "active"),  # Insert
    ]
    incremental_df = spark.createDataFrame(incremental_data, customer_df.schema)
    
    # Merge/Upsert
    processor.load_data(
        source_df=incremental_df,
        target_table="customers_merge",
        strategy="merge_upsert",
        target_catalog="iceberg",
        merge_keys=["customer_id"]
    )
    print("✓ Data merged/upserted")
    
    # Verify
    result = spark.table("iceberg.customers_merge")
    print(f"Total records: {result.count()}")
    result.show()


def example_scd_type2(spark: SparkSession, processor: BatchProcessor):
    """Example 3: SCD Type 2 strategy."""
    print("\n=== Example 3: SCD Type 2 Strategy ===")
    
    customer_df, _ = create_sample_data(spark)
    
    # Load initial data
    processor.load_data(
        source_df=customer_df,
        target_table="customers_scd2",
        strategy="scd_type2",
        target_catalog="iceberg",
        business_keys=["customer_id"]
    )
    print("✓ Initial data loaded with SCD Type 2")
    
    # Create changed data (customer 1 changed city, customer 2 changed status)
    changed_data = [
        (1, "Nguyễn Văn A", "nguyenvana@example.com", "Hải Phòng", "active"),  # City changed
        (2, "Trần Thị B", "tranthib@example.com", "Hồ Chí Minh", "premium"),  # Status changed
        (5, "Hoàng Văn E", "hoangvane@example.com", "Nha Trang", "active"),  # New record
    ]
    changed_df = spark.createDataFrame(changed_data, customer_df.schema)
    
    # Load with SCD Type 2
    processor.load_data(
        source_df=changed_df,
        target_table="customers_scd2",
        strategy="scd_type2",
        target_catalog="iceberg",
        business_keys=["customer_id"]
    )
    print("✓ Changes loaded with SCD Type 2")
    
    # Verify - show current and historical records
    result = spark.table("iceberg.customers_scd2")
    print(f"Total records (including history): {result.count()}")
    print("\nAll records (including historical):")
    result.orderBy("customer_id", "effective_date").show(truncate=False)
    
    print("\nCurrent records only:")
    result.filter("is_current = true").show(truncate=False)


def example_clickhouse_loading(spark: SparkSession, processor: BatchProcessor):
    """Example: Loading to ClickHouse."""
    print("\n=== Example: Loading to ClickHouse ===")
    
    customer_df, order_df = create_sample_data(spark)
    
    # Load to ClickHouse using delete_insert
    processor.load_data(
        source_df=customer_df,
        target_table="default.customers",
        strategy="delete_insert",
        target_catalog="clickhouse"
    )
    print("✓ Data loaded to ClickHouse")
    
    # Load orders with merge/upsert
    processor.load_data(
        source_df=order_df,
        target_table="default.orders",
        strategy="merge_upsert",
        target_catalog="clickhouse",
        merge_keys=["order_id"]
    )
    print("✓ Orders merged to ClickHouse")


if __name__ == "__main__":
    # Create Spark session with all catalogs
    spark = SparkConnector.create_with_all_catalogs("LoadingExamples")
    processor = BatchProcessor(spark)
    
    try:
        # Run examples
        example_delete_insert(spark, processor)
        example_merge_upsert(spark, processor)
        example_scd_type2(spark, processor)
        example_clickhouse_loading(spark, processor)
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

