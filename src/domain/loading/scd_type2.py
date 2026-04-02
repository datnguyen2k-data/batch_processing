"""SCD Type 2 domain calculation engine."""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, current_timestamp
from typing import List, Optional, Dict
from datetime import datetime

class ScdType2Evaluator:
    """
    Pure mathematical calculator for SCD Type 2 logic.
    Computes sets of records that need inserting vs expiring without touching physical DB I/O.
    """
    
    def __init__(self, business_keys: List[str], effective_date: Optional[datetime] = None):
        self.business_keys = business_keys
        self.effective_date_col = "effective_date"
        self.expiry_date_col = "expiry_date"
        self.is_current_col = "is_current"
        self.effective_date = effective_date or datetime.now()

    def evaluate(self, source_df: DataFrame, active_target_df: DataFrame) -> Dict[str, DataFrame]:
        """
        Evaluate source data against current active data to identify inserts and expires.
        
        Args:
            source_df: New data coming into the pipeline.
            active_target_df: Currently active records in the dimension table.
            
        Returns:
            Dict containing:
              - 'to_insert': DataFrame of new records to insert.
              - 'to_expire': DataFrame of old active records that must be expired.
        """
        # 1. Format source
        formatted_source = source_df.withColumn(self.effective_date_col, lit(self.effective_date)) \
                                    .withColumn(self.is_current_col, lit(True))
        
        # 2. To build generic computation, alias the dataframes
        source_alias = formatted_source.alias("s")
        target_alias = active_target_df.alias("t")

        # 3. Join condition
        join_conditions = [col(f"s.{k}") == col(f"t.{k}") for k in self.business_keys]
        join_expr = None
        for expr in join_conditions:
            join_expr = expr if join_expr is None else join_expr & expr
            
        # 4. Check for changed columns (exclude SCD control columns and keys)
        scd_columns = {self.effective_date_col, self.expiry_date_col, self.is_current_col}
        data_columns = [
            c for c in source_df.columns 
            if c not in self.business_keys and c not in scd_columns
        ]
        
        change_expr = None
        for c in data_columns:
            # simple COALESCE approximation using when/otherwise to handle null changes
            cond = ~((col(f"s.{c}").eqNullSafe(col(f"t.{c}"))))
            change_expr = cond if change_expr is None else change_expr | cond

        if change_expr is None:
            change_expr = lit(False)

        # 5. Changed Records = Inner Join + Changed Data
        changed_df = source_alias.join(target_alias, join_expr, "inner") \
                                 .filter(change_expr) \
                                 .select("s.*")
                                 
        # 6. New Records = Left Anti Join
        new_df = source_alias.join(target_alias, join_expr, "left_anti").select("s.*")
        
        # Payload out
        records_to_insert = new_df.unionByName(changed_df)
        records_to_expire = changed_df
        
        return {
            "to_insert": records_to_insert,
            "to_expire": records_to_expire,
            "effective_date": self.effective_date
        }
