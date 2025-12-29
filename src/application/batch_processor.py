"""Application service for batch processing."""
from pyspark.sql import SparkSession, DataFrame
from src.domain.loading import (
    DeleteInsertStrategy,
    MergeUpsertStrategy,
    SCDType2Strategy
)
from src.domain.join import (
    BasicJoinStrategy,
    AdvancedJoinStrategy,
    BroadcastJoinStrategy,
    BucketJoinStrategy
)
from typing import Optional, List, Dict, Any


class BatchProcessor:
    """Application service orchestrating batch processing operations."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.loading_strategies = {
            "delete_insert": DeleteInsertStrategy(spark),
            "merge_upsert": MergeUpsertStrategy(spark),
            "scd_type2": SCDType2Strategy(spark),
        }
        self.join_strategies = {
            "basic": BasicJoinStrategy(spark),
            "advanced": AdvancedJoinStrategy(spark),
            "broadcast": BroadcastJoinStrategy(spark),
            "bucket": BucketJoinStrategy(spark),
        }
    
    def load_data(
        self,
        source_df: DataFrame,
        target_table: str,
        strategy: str = "delete_insert",
        target_catalog: str = "iceberg",
        **kwargs
    ) -> None:
        """
        Load data using specified strategy.
        
        Args:
            source_df: Source DataFrame
            target_table: Target table name
            strategy: Loading strategy (delete_insert, merge_upsert, scd_type2)
            target_catalog: Target catalog name
            **kwargs: Strategy-specific parameters
        """
        if strategy not in self.loading_strategies:
            raise ValueError(f"Unknown strategy: {strategy}")
        
        strategy_instance = self.loading_strategies[strategy]
        strategy_instance.load(source_df, target_table, target_catalog, **kwargs)
    
    def join_data(
        self,
        left_df: DataFrame,
        right_df: DataFrame,
        join_keys: List[str],
        strategy: str = "basic",
        join_type: str = "inner",
        **kwargs
    ) -> DataFrame:
        """
        Join two DataFrames using specified strategy.
        
        Args:
            left_df: Left DataFrame
            right_df: Right DataFrame
            join_keys: Join keys
            strategy: Join strategy (basic, advanced, broadcast, bucket)
            join_type: Join type (inner, left, right, outer)
            **kwargs: Strategy-specific parameters
        
        Returns:
            Joined DataFrame
        """
        if strategy not in self.join_strategies:
            raise ValueError(f"Unknown join strategy: {strategy}")
        
        strategy_instance = self.join_strategies[strategy]
        
        if strategy == "basic":
            method_map = {
                "inner": strategy_instance.inner_join,
                "left": strategy_instance.left_join,
                "right": strategy_instance.right_join,
                "outer": strategy_instance.full_outer_join,
            }
            method = method_map.get(join_type, strategy_instance.inner_join)
            return method(left_df, right_df, join_keys, **kwargs)
        elif strategy == "broadcast":
            return strategy_instance.broadcast_join(
                left_df, right_df, join_keys, join_type, **kwargs
            )
        else:
            # For advanced strategies, use custom parameters
            return strategy_instance.conditional_join(
                left_df, right_df, kwargs.get("join_condition", ""), **kwargs
            )

