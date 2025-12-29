"""Join strategies from basic to advanced."""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, broadcast, when, coalesce
from typing import List, Optional, Dict, Any


class BasicJoinStrategy:
    """Basic join operations."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def inner_join(
        self,
        left_df: DataFrame,
        right_df: DataFrame,
        join_keys: List[str],
        select_columns: Optional[List[str]] = None
    ) -> DataFrame:
        """
        Perform inner join between two DataFrames.
        
        Args:
            left_df: Left DataFrame
            right_df: Right DataFrame
            join_keys: List of column names to join on
            select_columns: Optional list of columns to select in result
        
        Returns:
            Joined DataFrame
        """
        join_condition = [left_df[key] == right_df[key] for key in join_keys]
        result = left_df.join(right_df, join_condition, "inner")
        
        if select_columns:
            result = result.select(*select_columns)
        
        return result
    
    def left_join(
        self,
        left_df: DataFrame,
        right_df: DataFrame,
        join_keys: List[str],
        select_columns: Optional[List[str]] = None
    ) -> DataFrame:
        """Perform left join."""
        join_condition = [left_df[key] == right_df[key] for key in join_keys]
        result = left_df.join(right_df, join_condition, "left")
        
        if select_columns:
            result = result.select(*select_columns)
        
        return result
    
    def right_join(
        self,
        left_df: DataFrame,
        right_df: DataFrame,
        join_keys: List[str],
        select_columns: Optional[List[str]] = None
    ) -> DataFrame:
        """Perform right join."""
        join_condition = [left_df[key] == right_df[key] for key in join_keys]
        result = left_df.join(right_df, join_condition, "right")
        
        if select_columns:
            result = result.select(*select_columns)
        
        return result
    
    def full_outer_join(
        self,
        left_df: DataFrame,
        right_df: DataFrame,
        join_keys: List[str],
        select_columns: Optional[List[str]] = None
    ) -> DataFrame:
        """Perform full outer join."""
        join_condition = [left_df[key] == right_df[key] for key in join_keys]
        result = left_df.join(right_df, join_condition, "outer")
        
        if select_columns:
            result = result.select(*select_columns)
        
        return result


class AdvancedJoinStrategy:
    """Advanced join operations with complex conditions."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def conditional_join(
        self,
        left_df: DataFrame,
        right_df: DataFrame,
        join_condition: str,
        select_columns: Optional[List[str]] = None
    ) -> DataFrame:
        """
        Perform join with custom SQL condition.
        
        Args:
            left_df: Left DataFrame
            right_df: Right DataFrame
            join_condition: SQL condition string (e.g., "left.id = right.id AND left.date >= right.start_date")
            select_columns: Optional list of columns to select
        
        Returns:
            Joined DataFrame
        """
        left_df.createOrReplaceTempView("left_table")
        right_df.createOrReplaceTempView("right_table")
        
        select_clause = "*" if not select_columns else ", ".join(select_columns)
        
        sql = f"""
            SELECT {select_clause}
            FROM left_table
            INNER JOIN right_table
            ON {join_condition}
        """
        
        result = self.spark.sql(sql)
        return result
    
    def range_join(
        self,
        left_df: DataFrame,
        right_df: DataFrame,
        left_key: str,
        right_key: str,
        range_condition: str = ">=",
        select_columns: Optional[List[str]] = None
    ) -> DataFrame:
        """
        Perform range join (e.g., date ranges, numeric ranges).
        
        Args:
            left_df: Left DataFrame
            right_df: Right DataFrame
            left_key: Key column in left DataFrame
            right_key: Key column in right DataFrame
            range_condition: Range condition (>=, <=, >, <)
            select_columns: Optional list of columns to select
        
        Returns:
            Joined DataFrame
        """
        left_df.createOrReplaceTempView("left_table")
        right_df.createOrReplaceTempView("right_table")
        
        select_clause = "*" if not select_columns else ", ".join(select_columns)
        
        sql = f"""
            SELECT {select_clause}
            FROM left_table
            INNER JOIN right_table
            ON left_table.{left_key} {range_condition} right_table.{right_key}
        """
        
        result = self.spark.sql(sql)
        return result
    
    def multi_table_join(
        self,
        dataframes: List[DataFrame],
        join_conditions: List[Dict[str, Any]],
        select_columns: Optional[List[str]] = None
    ) -> DataFrame:
        """
        Perform join across multiple tables.
        
        Args:
            dataframes: List of DataFrames to join
            join_conditions: List of join conditions, each as dict:
                {
                    "left_table": "table1",
                    "right_table": "table2",
                    "left_key": "id",
                    "right_key": "id",
                    "join_type": "inner"
                }
            select_columns: Optional list of columns to select
        
        Returns:
            Joined DataFrame
        """
        # Register all DataFrames as temp views
        for i, df in enumerate(dataframes):
            df.createOrReplaceTempView(f"table_{i}")
        
        # Build SQL query
        select_clause = "*" if not select_columns else ", ".join(select_columns)
        
        sql_parts = [f"SELECT {select_clause} FROM table_0"]
        
        for condition in join_conditions:
            left_table = condition.get("left_table", f"table_{dataframes.index(dataframes[0])}")
            right_table = condition.get("right_table", f"table_{dataframes.index(dataframes[1])}")
            left_key = condition["left_key"]
            right_key = condition["right_key"]
            join_type = condition.get("join_type", "inner")
            
            sql_parts.append(
                f"{join_type.upper()} JOIN {right_table} "
                f"ON {left_table}.{left_key} = {right_table}.{right_key}"
            )
        
        sql = " ".join(sql_parts)
        result = self.spark.sql(sql)
        return result
    
    def coalesce_join(
        self,
        left_df: DataFrame,
        right_df: DataFrame,
        join_keys: List[str],
        coalesce_columns: Dict[str, List[str]]
    ) -> DataFrame:
        """
        Join with coalesce operation (prefer left, fallback to right).
        
        Args:
            left_df: Left DataFrame
            right_df: Right DataFrame
            join_keys: Join keys
            coalesce_columns: Dict mapping result column name to [left_col, right_col]
        
        Returns:
            Joined DataFrame with coalesced columns
        """
        result = left_df.join(right_df, join_keys, "outer")
        
        for result_col, (left_col, right_col) in coalesce_columns.items():
            result = result.withColumn(
                result_col,
                coalesce(col(left_col), col(right_col))
            )
        
        return result


class BroadcastJoinStrategy:
    """Broadcast join for small tables."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def broadcast_join(
        self,
        large_df: DataFrame,
        small_df: DataFrame,
        join_keys: List[str],
        join_type: str = "inner",
        broadcast_threshold_mb: int = 10
    ) -> DataFrame:
        """
        Perform broadcast join (optimized for small right table).
        
        Args:
            large_df: Large DataFrame (left)
            small_df: Small DataFrame (right) - will be broadcast
            join_keys: Join keys
            join_type: Join type (inner, left, right, outer)
            broadcast_threshold_mb: Size threshold in MB for auto-broadcast
        
        Returns:
            Joined DataFrame
        """
        # Set broadcast threshold
        self.spark.conf.set("spark.sql.autoBroadcastJoinThreshold", broadcast_threshold_mb * 1024 * 1024)
        
        # Broadcast small DataFrame
        broadcast_df = broadcast(small_df)
        
        join_condition = [large_df[key] == broadcast_df[key] for key in join_keys]
        result = large_df.join(broadcast_df, join_condition, join_type)
        
        return result


class BucketJoinStrategy:
    """Bucket join for pre-partitioned tables."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def create_bucketed_table(
        self,
        df: DataFrame,
        table_name: str,
        bucket_columns: List[str],
        num_buckets: int = 10,
        catalog: str = "iceberg"
    ) -> None:
        """
        Create a bucketed table for optimized joins.
        
        Args:
            df: DataFrame to bucket
            table_name: Target table name
            bucket_columns: Columns to bucket on
            num_buckets: Number of buckets
            catalog: Catalog name
        """
        full_table_name = f"{catalog}.{table_name}" if catalog else table_name
        
        # For Iceberg, use CLUSTERED BY
        if catalog == "iceberg":
            df.write \
                .format("iceberg") \
                .option("write-distribution-mode", "hash") \
                .option("write-distribution-mode", "range") \
                .saveAsTable(full_table_name)
        else:
            # For other catalogs, use Spark bucketing
            df.write \
                .bucketBy(num_buckets, *bucket_columns) \
                .saveAsTable(full_table_name)
    
    def bucketed_join(
        self,
        left_table: str,
        right_table: str,
        join_keys: List[str],
        left_catalog: str = "iceberg",
        right_catalog: str = "iceberg",
        join_type: str = "inner"
    ) -> DataFrame:
        """
        Perform join on bucketed tables (optimized).
        
        Args:
            left_table: Left table name
            right_table: Right table name
            join_keys: Join keys
            left_catalog: Left table catalog
            right_catalog: Right table catalog
            join_type: Join type
        
        Returns:
            Joined DataFrame
        """
        left_full = f"{left_catalog}.{left_table}" if left_catalog else left_table
        right_full = f"{right_catalog}.{right_table}" if right_catalog else right_table
        
        left_df = self.spark.table(left_full)
        right_df = self.spark.table(right_full)
        
        join_condition = [left_df[key] == right_df[key] for key in join_keys]
        result = left_df.join(right_df, join_condition, join_type)
        
        return result

