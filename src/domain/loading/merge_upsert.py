"""Merge/Upsert loading strategy."""
from pyspark.sql import SparkSession, DataFrame
from src.domain.loading.base_strategy import LoadingStrategy
from typing import List, Optional


class MergeUpsertStrategy(LoadingStrategy):
    """
    Merge/Upsert strategy: Update existing records or insert new ones.
    Suitable for incremental updates.
    """
    
    def load(
        self,
        source_df: DataFrame,
        target_table: str,
        target_catalog: str = "iceberg",
        merge_keys: Optional[List[str]] = None,
        **kwargs
    ) -> None:
        """
        Load data using merge/upsert strategy.
        
        Args:
            source_df: Source DataFrame to load
            target_table: Target table name
            target_catalog: Catalog name
            merge_keys: List of column names to use for matching records
            **kwargs: Additional parameters
        """
        if not merge_keys:
            raise ValueError("merge_keys must be provided for merge/upsert strategy")
        
        full_table_name = self._get_full_table_name(target_table, target_catalog)
        
        # Register source DataFrame as temporary view
        source_df.createOrReplaceTempView("source_data")
        
        if target_catalog == "iceberg":
            # Use Iceberg MERGE INTO syntax
            merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in merge_keys])
            update_set = ", ".join([
                f"target.{col} = source.{col}"
                for col in source_df.columns
                if col not in merge_keys
            ])
            insert_columns = ", ".join(source_df.columns)
            insert_values = ", ".join([f"source.{col}" for col in source_df.columns])
            
            merge_sql = f"""
            MERGE INTO {full_table_name} AS target
            USING source_data AS source
            ON {merge_condition}
            WHEN MATCHED THEN
                UPDATE SET {update_set}
            WHEN NOT MATCHED THEN
                INSERT ({insert_columns})
                VALUES ({insert_values})
            """
            
            self.spark.sql(merge_sql)
        else:
            # For non-Iceberg catalogs (like ClickHouse), use alternative approach
            # Read existing data
            try:
                existing_df = self.spark.table(full_table_name)
                
                # Create join condition
                join_condition = None
                for key in merge_keys:
                    if join_condition is None:
                        join_condition = f"existing.{key} = source.{key}"
                    else:
                        join_condition += f" AND existing.{key} = source.{key}"
                
                # Find records to update (existing records that match)
                existing_df.createOrReplaceTempView("existing_data")
                
                # Build join condition for SQL
                sql_join_condition = " AND ".join([
                    f"existing.{key} = source.{key}"
                    for key in merge_keys
                ])
                
                updated_df = self.spark.sql(f"""
                    SELECT source.*
                    FROM source_data source
                    INNER JOIN existing_data existing
                    ON {sql_join_condition}
                """)
                
                # Find records to insert (new records)
                new_df = self.spark.sql(f"""
                    SELECT source.*
                    FROM source_data source
                    LEFT ANTI JOIN existing_data existing
                    ON {sql_join_condition}
                """)
                
                # Find records to keep (existing records that don't match)
                keep_df = self.spark.sql(f"""
                    SELECT existing.*
                    FROM existing_data existing
                    LEFT ANTI JOIN source_data source
                    ON {sql_join_condition}
                """)
                
                # Union all: keep existing, update with new, insert new
                result_df = keep_df.union(updated_df).union(new_df)
                result_df.write.mode("overwrite").saveAsTable(full_table_name)
                
            except Exception:
                # Table doesn't exist, just insert
                source_df.write.mode("overwrite").saveAsTable(full_table_name)

