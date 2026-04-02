from src.domain.loading.data_port import ILoadingDataPort
from src.domain.loading.scd_type2 import ScdType2Evaluator
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, col, when
from typing import List, Optional
from datetime import datetime

class GenericSparkLoadingAdapter(ILoadingDataPort):
    """
    Standard DataFrame-based loading adapter.
    Uses DataFrame APIs like .overwrite() instead of native Delta/Iceberg SQL.
    Good for Parquet, ClickHouse, Postgres (via JDBC).
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def execute_delete_insert(
        self,
        source_df: DataFrame,
        full_table_name: str,
        filter_condition: Optional[str] = None
    ) -> None:
        try:
            existing_df = self.spark.table(full_table_name)
            if filter_condition:
                remaining_df = existing_df.filter(f"NOT ({filter_condition})")
                remaining_df.write.mode("overwrite").saveAsTable(full_table_name)
                source_df.write.mode("append").saveAsTable(full_table_name)
            else:
                source_df.write.mode("overwrite").saveAsTable(full_table_name)
        except Exception:
            # Table doesn't exist
            source_df.write.mode("overwrite").saveAsTable(full_table_name)

    def execute_merge_upsert(
        self,
        source_df: DataFrame,
        full_table_name: str,
        merge_keys: List[str]
    ) -> None:
        try:
            existing_df = self.spark.table(full_table_name)
            
            # Simulated upsert using Left Anti & Inner joins on dataframes
            join_cond = None
            for key in merge_keys:
                cond = col(f"e.{key}") == col(f"s.{key}")
                join_cond = cond if join_cond is None else join_cond & cond
                
            e = existing_df.alias("e")
            s = source_df.alias("s")
            
            # Anti join to keep old records
            kept_df = e.join(s, join_cond, "left_anti").select("e.*")
            
            # Final result is old records + all incoming source
            result_df = kept_df.unionByName(source_df)
            result_df.write.mode("overwrite").saveAsTable(full_table_name)
            
        except Exception:
            source_df.write.mode("overwrite").saveAsTable(full_table_name)

    def execute_scd_type2(
        self,
        source_df: DataFrame,
        full_table_name: str,
        business_keys: List[str],
        effective_date: Optional[datetime] = None
    ) -> None:
        evaluator = ScdType2Evaluator(business_keys, effective_date)
        try:
            active_target_df = self.spark.table(full_table_name).filter("is_current = true")
            payload = evaluator.evaluate(source_df, active_target_df)
            
            # Emulate physical update + insert via dataframe overlay
            all_existing = self.spark.table(full_table_name)
            
            # Determine expired keys
            expired_keys_df = payload["to_expire"].select(*business_keys).withColumn("_is_expiring", lit(True))
            
            joined = all_existing.join(expired_keys_df, business_keys, "left_outer")
            
            # Apply expiration changes
            updated_existing = joined.withColumn(
                "expiry_date",
                when(col("_is_expiring") & col("is_current"), lit(payload["effective_date"]))
                .otherwise(col("expiry_date"))
            ).withColumn(
                "is_current",
                when(col("_is_expiring") & col("is_current"), lit(False))
                .otherwise(col("is_current"))
            ).drop("_is_expiring")
            
            # Append new valid rows
            to_insert = payload["to_insert"].withColumn("expiry_date", lit(None).cast("timestamp"))
            
            final_df = updated_existing.unionByName(to_insert)
            final_df.write.mode("overwrite").saveAsTable(full_table_name)
            
        except Exception:
            # First time load
            to_insert = source_df.withColumn("effective_date", lit(evaluator.effective_date)) \
                                 .withColumn("is_current", lit(True)) \
                                 .withColumn("expiry_date", lit(None).cast("timestamp"))
            to_insert.write.mode("overwrite").saveAsTable(full_table_name)
