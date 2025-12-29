"""Delete and Insert loading strategy."""
from pyspark.sql import SparkSession, DataFrame
from src.domain.loading.base_strategy import LoadingStrategy
from typing import Optional


class DeleteInsertStrategy(LoadingStrategy):
    """
    Delete and Insert strategy: Delete existing data and insert new data.
    Suitable for full refresh scenarios.
    """
    
    def load(
        self,
        source_df: DataFrame,
        target_table: str,
        target_catalog: str = "iceberg",
        filter_condition: Optional[str] = None,
        **kwargs
    ) -> None:
        """
        Load data using delete and insert strategy.
        
        Args:
            source_df: Source DataFrame to load
            target_table: Target table name
            target_catalog: Catalog name
            filter_condition: Optional SQL condition to filter which rows to delete
                            (e.g., "date = '2024-01-01'")
            **kwargs: Additional parameters
        """
        full_table_name = self._get_full_table_name(target_table, target_catalog)
        
        # Check if table exists
        try:
            existing_df = self.spark.table(full_table_name)
            
            # Delete existing data based on filter condition
            if filter_condition:
                # For Iceberg, we can use DELETE WHERE
                if target_catalog == "iceberg":
                    self.spark.sql(f"DELETE FROM {full_table_name} WHERE {filter_condition}")
                else:
                    # For other catalogs, read, filter, and overwrite
                    remaining_df = existing_df.filter(f"NOT ({filter_condition})")
                    remaining_df.write.mode("overwrite").saveAsTable(full_table_name)
                    # Then insert new data
                    source_df.write.mode("append").saveAsTable(full_table_name)
                    return
            else:
                # Delete all data - truncate or overwrite
                if target_catalog == "iceberg":
                    self.spark.sql(f"TRUNCATE TABLE {full_table_name}")
                else:
                    # For non-Iceberg, just overwrite
                    source_df.write.mode("overwrite").saveAsTable(full_table_name)
                    return
        except Exception:
            # Table doesn't exist, create it
            pass
        
        # Insert new data
        source_df.write.mode("append").saveAsTable(full_table_name)

