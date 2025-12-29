"""SCD Type 2 loading strategy for historical tracking."""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, lit, current_timestamp, max as spark_max
from src.domain.loading.base_strategy import LoadingStrategy
from typing import List, Optional
from datetime import datetime


class SCDType2Strategy(LoadingStrategy):
    """
    Slowly Changing Dimension Type 2 strategy.
    Maintains historical records by adding effective date columns.
    """
    
    def __init__(self, spark: SparkSession):
        super().__init__(spark)
        self.effective_date_col = "effective_date"
        self.expiry_date_col = "expiry_date"
        self.is_current_col = "is_current"
    
    def load(
        self,
        source_df: DataFrame,
        target_table: str,
        target_catalog: str = "iceberg",
        business_keys: Optional[List[str]] = None,
        effective_date: Optional[datetime] = None,
        **kwargs
    ) -> None:
        """
        Load data using SCD Type 2 strategy.
        
        Args:
            source_df: Source DataFrame to load
            target_table: Target table name
            target_catalog: Catalog name
            business_keys: List of column names that uniquely identify a business entity
            effective_date: Effective date for the new records (defaults to current timestamp)
            **kwargs: Additional parameters
        """
        if not business_keys:
            raise ValueError("business_keys must be provided for SCD Type 2 strategy")
        
        full_table_name = self._get_full_table_name(target_table, target_catalog)
        
        # Add effective date to source data
        if effective_date:
            source_df = source_df.withColumn(
                self.effective_date_col,
                lit(effective_date)
            )
        else:
            source_df = source_df.withColumn(
                self.effective_date_col,
                current_timestamp()
            )
        
        source_df = source_df.withColumn(self.is_current_col, lit(True))
        source_df.createOrReplaceTempView("source_data")
        
        try:
            existing_df = self.spark.table(full_table_name)
            existing_df.createOrReplaceTempView("existing_data")
            
            # Build join condition on business keys
            join_condition = " AND ".join([
                f"existing.{key} = source.{key}"
                for key in business_keys
            ])
            
            # Find changed records (records that exist but have different values)
            # Compare all non-key, non-SCD columns
            scd_columns = [self.effective_date_col, self.expiry_date_col, self.is_current_col]
            data_columns = [
                col_name for col_name in source_df.columns
                if col_name not in business_keys and col_name not in scd_columns
            ]
            
            # Create change detection condition
            change_conditions = []
            for data_col in data_columns:
                change_conditions.append(f"COALESCE(existing.{data_col}, '') != COALESCE(source.{data_col}, '')")
            
            change_condition = " OR ".join(change_conditions) if change_conditions else "FALSE"
            
            # SQL to identify changed records
            changed_records_sql = f"""
                SELECT DISTINCT source.*
                FROM source_data source
                INNER JOIN existing_data existing
                ON {join_condition}
                WHERE existing.{self.is_current_col} = true
                AND ({change_condition})
            """
            
            changed_df = self.spark.sql(changed_records_sql)
            
            # Expire old records (set expiry_date and is_current = false)
            if changed_df.count() > 0:
                changed_df.createOrReplaceTempView("changed_data")
                
                # Update existing records to expired
                expire_sql = f"""
                    UPDATE {full_table_name}
                    SET {self.expiry_date_col} = changed.{self.effective_date_col},
                        {self.is_current_col} = false
                    FROM changed_data changed
                    WHERE {join_condition}
                    AND {full_table_name}.{self.is_current_col} = true
                """
                
                if target_catalog == "iceberg":
                    self.spark.sql(expire_sql)
                else:
                    # For non-Iceberg, use DataFrame operations
                    existing_df = existing_df.join(
                        changed_df.select(*business_keys, self.effective_date_col.alias("new_effective_date")),
                        business_keys,
                        "inner"
                    )
                    existing_df = existing_df.withColumn(
                        self.expiry_date_col,
                        when(col(self.is_current_col) == True, col("new_effective_date"))
                        .otherwise(col(self.expiry_date_col))
                    ).withColumn(
                        self.is_current_col,
                        when(col(self.is_current_col) == True, lit(False))
                        .otherwise(col(self.is_current_col))
                    ).drop("new_effective_date")
            
            # Find new records (records that don't exist)
            new_records_sql = f"""
                SELECT source.*
                FROM source_data source
                LEFT ANTI JOIN existing_data existing
                ON {join_condition}
            """
            new_df = self.spark.sql(new_records_sql)
            
            # Insert new and changed records
            records_to_insert = changed_df.union(new_df)
            if records_to_insert.count() > 0:
                records_to_insert = records_to_insert.withColumn(
                    self.expiry_date_col,
                    lit(None).cast("timestamp")
                )
                records_to_insert.write.mode("append").saveAsTable(full_table_name)
            
        except Exception:
            # Table doesn't exist, create it with all records as current
            source_df = source_df.withColumn(
                self.expiry_date_col,
                lit(None).cast("timestamp")
            )
            source_df.write.mode("overwrite").saveAsTable(full_table_name)

