from src.domain.loading.data_port import ILoadingDataPort
from src.shared.config import ClickHouseConfig
from pyspark.sql import SparkSession, DataFrame
from typing import List, Optional
from datetime import datetime

class ClickHouseLoadingAdapter(ILoadingDataPort):
    """
    Adapter for ClickHouse databases.
    ClickHouse ReplacingMergeTree handles upsert deduplication automatically 
    in the background, so we only need to Append data.
    Overwriting the whole table is not native and drops the table schema (Missing ORDER BY).
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def execute_delete_insert(
        self,
        source_df: DataFrame,
        full_table_name: str,
        filter_condition: Optional[str] = None
    ) -> None:
        if filter_condition:
            # ClickHouse supports ALTER TABLE ... DELETE WHERE
            self.spark.sql(f"ALTER TABLE {full_table_name} DELETE WHERE {filter_condition}")
        else:
            # Truncate
            self.spark.sql(f"TRUNCATE TABLE {full_table_name}")
            
        source_df.writeTo(full_table_name).append()

    def execute_merge_upsert(
        self,
        source_df: DataFrame,
        full_table_name: str,
        merge_keys: List[str]
    ) -> None:
        # For ClickHouse catalog in Spark 3, writeTo().append() handles Appends cleanly.
        # Deduplication will rely on the table's MergeTree/ReplacingMergeTree configuration natively.
        source_df.writeTo(full_table_name).append()

    def execute_scd_type2(
        self,
        source_df: DataFrame,
        full_table_name: str,
        business_keys: List[str],
        effective_date: Optional[datetime] = None
    ) -> None:
        # SCD Type 2 implementation in Clickhouse typically means Appending new versions, 
        # then Async ALTER TABLE ... UPDATE to expire old ones, 
        # but for simplicity we rely on ReplacingMergeTree if versioned.
        raise NotImplementedError("SCD Type 2 native adapter for Clickhouse not yet implemented.")

