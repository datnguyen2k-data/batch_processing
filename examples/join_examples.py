"""
Examples demonstrating different join strategies from basic to advanced:
1. Basic joins (inner, left, right, outer)
2. Advanced joins (conditional, range, multi-table, coalesce)
3. Broadcast joins
4. Bucket joins
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import col, lit
from src.infrastructure.connectors.spark_connector import SparkConnector
from pyspark.sql.functions import to_date as spark_to_date   
from src.application.batch_processor import BatchProcessor
from src.domain.join import (
    BasicJoinStrategy,
    AdvancedJoinStrategy,
    BroadcastJoinStrategy,
    BucketJoinStrategy
)


def create_sample_data_for_joins(spark: SparkSession):
    """Create sample DataFrames for join examples."""
    # Customers table
    customers_schema = StructType([
        StructField("customer_id", IntegerType(), False),
        StructField("customer_name", StringType(), True),
        StructField("city", StringType(), True),
        StructField("registration_date", DateType(), True),
    ])
    
    customers_data = [
        (1, "Nguyễn Văn A", "Hà Nội", "2023-01-15"),
        (2, "Trần Thị B", "Hồ Chí Minh", "2023-02-20"),
        (3, "Lê Văn C", "Đà Nẵng", "2023-03-10"),
        (4, "Phạm Thị D", "Cần Thơ", "2023-04-05"),
    ]
    
    customers_df = spark.createDataFrame(
        [(r[0], r[1], r[2], spark_to_date(lit(r[3]))) for r in customers_data],
        customers_schema
    )
    
    # Orders table
    orders_schema = StructType([
        StructField("order_id", IntegerType(), False),
        StructField("customer_id", IntegerType(), False),
        StructField("product_name", StringType(), True),
        StructField("amount", IntegerType(), True),
        StructField("order_date", DateType(), True),
    ])
    
    orders_data = [
        (101, 1, "Laptop", 15000000, "2024-01-10"),
        (102, 1, "Mouse", 500000, "2024-01-12"),
        (103, 2, "Keyboard", 1200000, "2024-01-15"),
        (104, 5, "Monitor", 5000000, "2024-01-20"),  # customer_id 5 doesn't exist
    ]
    
    orders_df = spark.createDataFrame(
        [(r[0], r[1], r[2], r[3], spark_to_date(lit(r[4]))) for r in orders_data],
        orders_schema
    )
    
    # Products table
    products_schema = StructType([
        StructField("product_id", IntegerType(), False),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", IntegerType(), True),
    ])
    
    products_data = [
        (1, "Laptop", "Electronics", 15000000),
        (2, "Mouse", "Accessories", 500000),
        (3, "Keyboard", "Accessories", 1200000),
        (4, "Monitor", "Electronics", 5000000),
    ]
    
    products_df = spark.createDataFrame(products_data, products_schema)
    
    # Date ranges table (for range join example)
    date_ranges_schema = StructType([
        StructField("period_id", IntegerType(), False),
        StructField("start_date", DateType(), True),
        StructField("end_date", DateType(), True),
        StructField("period_name", StringType(), True),
    ])
    
    date_ranges_data = [
        (1, "2024-01-01", "2024-01-15", "Early January"),
        (2, "2024-01-16", "2024-01-31", "Late January"),
    ]
    
    date_ranges_df = spark.createDataFrame(
        [(r[0], spark_to_date(lit(r[1])), spark_to_date(lit(r[2])), r[3]) for r in date_ranges_data],
        date_ranges_schema
    )
    
    return customers_df, orders_df, products_df, date_ranges_df


def example_basic_joins(spark: SparkSession):
    """Example 1: Basic join operations."""
    print("\n=== Example 1: Basic Joins ===")
    
    customers_df, orders_df, _, _ = create_sample_data_for_joins(spark)
    basic_join = BasicJoinStrategy(spark)
    
    # Inner Join
    print("\n1.1 Inner Join:")
    inner_result = basic_join.inner_join(
        customers_df,
        orders_df,
        join_keys=["customer_id"],
        select_columns=["customer_id", "customer_name", "order_id", "product_name", "amount"]
    )
    inner_result.show()
    
    # Left Join
    print("\n1.2 Left Join (all customers, including those without orders):")
    left_result = basic_join.left_join(
        customers_df,
        orders_df,
        join_keys=["customer_id"],
        select_columns=["customer_id", "customer_name", "order_id", "product_name"]
    )
    left_result.show()
    
    # Right Join
    print("\n1.3 Right Join (all orders, including those without customers):")
    right_result = basic_join.right_join(
        customers_df,
        orders_df,
        join_keys=["customer_id"],
        select_columns=["customer_id", "customer_name", "order_id", "product_name"]
    )
    right_result.show()
    
    # Full Outer Join
    print("\n1.4 Full Outer Join:")
    outer_result = basic_join.full_outer_join(
        customers_df,
        orders_df,
        join_keys=["customer_id"],
        select_columns=["customer_id", "customer_name", "order_id", "product_name"]
    )
    outer_result.show()


def example_advanced_joins(spark: SparkSession):
    """Example 2: Advanced join operations."""
    print("\n=== Example 2: Advanced Joins ===")
    
    customers_df, orders_df, products_df, date_ranges_df = create_sample_data_for_joins(spark)
    advanced_join = AdvancedJoinStrategy(spark)
    
    # Conditional Join (with date range)
    print("\n2.1 Conditional Join (orders within date range):")
    conditional_result = advanced_join.conditional_join(
        orders_df,
        date_ranges_df,
        join_condition=(
            "orders.order_date >= date_ranges.start_date "
            "AND orders.order_date <= date_ranges.end_date"
        ),
        select_columns=["order_id", "order_date", "period_name", "start_date", "end_date"]
    )
    conditional_result.show()
    
    # Range Join
    print("\n2.2 Range Join (orders after registration date):")
    range_result = advanced_join.range_join(
        orders_df,
        customers_df,
        left_key="order_date",
        right_key="registration_date",
        range_condition=">=",
        select_columns=["order_id", "order_date", "customer_name", "registration_date"]
    )
    range_result.show()
    
    # Multi-table Join
    print("\n2.3 Multi-table Join (customers -> orders -> products):")
    multi_result = advanced_join.multi_table_join(
        dataframes=[customers_df, orders_df, products_df],
        join_conditions=[
            {
                "left_table": "table_0",
                "right_table": "table_1",
                "left_key": "customer_id",
                "right_key": "customer_id",
                "join_type": "inner"
            },
            {
                "left_table": "table_1",
                "right_table": "table_2",
                "left_key": "product_name",
                "right_key": "product_name",
                "join_type": "inner"
            }
        ],
        select_columns=[
            "table_0.customer_name",
            "table_1.order_id",
            "table_1.product_name",
            "table_1.amount",
            "table_2.category"
        ]
    )
    multi_result.show()
    
    # Coalesce Join
    print("\n2.4 Coalesce Join (prefer left, fallback to right):")
    # Create two DataFrames with overlapping data
    left_data = spark.createDataFrame([
        (1, "Nguyễn Văn A", "Hà Nội", None),
        (2, "Trần Thị B", None, "active"),
    ], ["customer_id", "customer_name", "city", "status"])
    
    right_data = spark.createDataFrame([
        (1, None, "Hà Nội", "premium"),
        (2, "Trần Thị B", "Hồ Chí Minh", None),
    ], ["customer_id", "customer_name", "city", "status"])
    
    coalesce_result = advanced_join.coalesce_join(
        left_data,
        right_data,
        join_keys=["customer_id"],
        coalesce_columns={
            "final_name": ["customer_name", "customer_name"],
            "final_city": ["city", "city"],
            "final_status": ["status", "status"]
        }
    )
    coalesce_result.select(
        "customer_id",
        col("final_name").alias("customer_name"),
        col("final_city").alias("city"),
        col("final_status").alias("status")
    ).show()


def example_broadcast_join(spark: SparkSession):
    """Example 3: Broadcast join for small tables."""
    print("\n=== Example 3: Broadcast Join ===")
    
    customers_df, orders_df, _, _ = create_sample_data_for_joins(spark)
    broadcast_join = BroadcastJoinStrategy(spark)
    
    # Broadcast join (customers is small, orders is large)
    print("Broadcast Join (small customers table with large orders table):")
    broadcast_result = broadcast_join.broadcast_join(
        large_df=orders_df,
        small_df=customers_df,
        join_keys=["customer_id"],
        join_type="inner",
        broadcast_threshold_mb=10
    )
    broadcast_result.select(
        "order_id",
        "customer_name",
        "product_name",
        "amount"
    ).show()


def example_bucket_join(spark: SparkSession):
    """Example 4: Bucket join for pre-partitioned tables."""
    print("\n=== Example 4: Bucket Join ===")
    
    customers_df, orders_df, _, _ = create_sample_data_for_joins(spark)
    bucket_join = BucketJoinStrategy(spark)
    
    # Create bucketed tables
    print("Creating bucketed tables...")
    try:
        bucket_join.create_bucketed_table(
            df=customers_df,
            table_name="customers_bucketed",
            bucket_columns=["customer_id"],
            num_buckets=4,
            catalog="iceberg"
        )
        print("✓ Customers table bucketed")
        
        bucket_join.create_bucketed_table(
            df=orders_df,
            table_name="orders_bucketed",
            bucket_columns=["customer_id"],
            num_buckets=4,
            catalog="iceberg"
        )
        print("✓ Orders table bucketed")
        
        # Perform bucketed join
        print("\nPerforming bucketed join:")
        bucketed_result = bucket_join.bucketed_join(
            left_table="customers_bucketed",
            right_table="orders_bucketed",
            join_keys=["customer_id"],
            left_catalog="iceberg",
            right_catalog="iceberg",
            join_type="inner"
        )
        bucketed_result.select(
            "customer_id",
            "customer_name",
            "order_id",
            "product_name"
        ).show()
        
    except Exception as e:
        print(f"Note: Bucket join requires table creation. Error: {e}")
        print("This is expected if tables don't exist yet.")


def example_complex_join_scenario(spark: SparkSession):
    """Example 5: Complex real-world join scenario."""
    print("\n=== Example 5: Complex Join Scenario ===")
    
    customers_df, orders_df, products_df, _ = create_sample_data_for_joins(spark)
    basic_join = BasicJoinStrategy(spark)
    advanced_join = AdvancedJoinStrategy(spark)
    
    # Step 1: Join customers with orders
    customer_orders = basic_join.inner_join(
        customers_df,
        orders_df,
        join_keys=["customer_id"]
    )
    
    # Step 2: Join with products using product name
    customer_orders.createOrReplaceTempView("customer_orders")
    products_df.createOrReplaceTempView("products")
    
    final_result = spark.sql("""
        SELECT 
            co.customer_id,
            co.customer_name,
            co.city,
            co.order_id,
            co.product_name,
            co.amount,
            p.category,
            p.price,
            (co.amount - p.price) as discount
        FROM customer_orders co
        INNER JOIN products p
        ON co.product_name = p.product_name
        ORDER BY co.customer_id, co.order_id
    """)
    
    print("Complex join result (customers -> orders -> products with discount calculation):")
    final_result.show()


if __name__ == "__main__":
    # Create Spark session with all catalogs
    spark = SparkConnector.create_with_all_catalogs("JoinExamples")
    
    try:
        # Run examples
        example_basic_joins(spark)
        example_advanced_joins(spark)
        example_broadcast_join(spark)
        example_bucket_join(spark)
        example_complex_join_scenario(spark)
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

