from src.domain.loading.data_port import ILoadingDataPort
from src.domain.loading.scd_type2 import ScdType2Evaluator
from pyspark.sql import SparkSession, DataFrame
from typing import List, Optional
from datetime import datetime

class IcebergLoadingAdapter(ILoadingDataPort):
    """
    Iceberg-optimized Loading Adapter utilizing native SQL MERGE INTO and DELETE WHERE.
    This guarantees ACID properties without shuffling all historical records.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def execute_delete_insert(
        self,
        source_df: DataFrame,
        full_table_name: str,
        filter_condition: Optional[str] = None
    ) -> None:
        if self._table_exists(full_table_name):
            if filter_condition:
                self.spark.sql(f"DELETE FROM {full_table_name} WHERE {filter_condition}")
            else:
                self.spark.sql(f"TRUNCATE TABLE {full_table_name}")
        
        source_df.write.mode("append").saveAsTable(full_table_name)

    def execute_merge_upsert(
        self,
        source_df: DataFrame,
        full_table_name: str,
        merge_keys: List[str]
    ) -> None:
        if not self._table_exists(full_table_name):
            source_df.write.mode("overwrite").saveAsTable(full_table_name)
            return
            
        source_df.createOrReplaceTempView("_iceberg_source_data")
        
        merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in merge_keys])
        update_set = ", ".join([f"target.{col} = source.{col}" for col in source_df.columns if col not in merge_keys])
        insert_columns = ", ".join(source_df.columns)
        insert_values = ", ".join([f"source.{col}" for col in source_df.columns])
        
        merge_sql = f"""
        MERGE INTO {full_table_name} AS target
        USING _iceberg_source_data AS source
        ON {merge_condition}
        WHEN MATCHED THEN
            UPDATE SET {update_set}
        WHEN NOT MATCHED THEN
            INSERT ({insert_columns})
            VALUES ({insert_values})
        """
        self.spark.sql(merge_sql)

    def execute_scd_type2(
        self,
        source_df: DataFrame,
        full_table_name: str,
        business_keys: List[str],
        effective_date: Optional[datetime] = None
    ) -> None:
        evaluator = ScdType2Evaluator(business_keys, effective_date)
        
        if not self._table_exists(full_table_name):
            # First load
            res = evaluator.evaluate(source_df, self.spark.createDataFrame([], source_df.schema))
            to_insert = res["to_insert"].withColumn("expiry_date", lit(None).cast("timestamp"))
            to_insert.write.mode("overwrite").saveAsTable(full_table_name)
            return

        active_target_df = self.spark.table(full_table_name).filter("is_current = true")
        payload = evaluator.evaluate(source_df, active_target_df)
        
        to_expire_df = payload["to_expire"]
        to_insert_df = payload["to_insert"]
        
        if to_expire_df.count() > 0:
            to_expire_df.createOrReplaceTempView("_iceberg_expire_data")
            join_condition = " AND ".join([f"target.{key} = changed.{key}" for key in business_keys])
            expire_sql = f"""
                UPDATE {full_table_name} target
                SET expiry_date = timestamp '{payload["effective_date"].strftime('%Y-%m-%d %H:%M:%S')}',
                    is_current = false
                FROM _iceberg_expire_data changed
                WHERE {join_condition}
                AND target.is_current = true
            """
            self.spark.sql(expire_sql)
            
        if to_insert_df.count() > 0:
            to_insert = to_insert_df.withColumn("expiry_date", lit(None).cast("timestamp"))
            to_insert.write.mode("append").saveAsTable(full_table_name)
            
    def _table_exists(self, full_table_name: str) -> bool:
        try:
            self.spark.table(full_table_name)
            return True
        except Exception:
            return False
