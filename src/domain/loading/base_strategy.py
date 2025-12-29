"""Base strategy interface for loading data."""
from abc import ABC, abstractmethod
from pyspark.sql import SparkSession, DataFrame
from typing import Dict, Any, Optional


class LoadingStrategy(ABC):
    """Abstract base class for all loading strategies."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    @abstractmethod
    def load(
        self,
        source_df: DataFrame,
        target_table: str,
        target_catalog: str = "iceberg",
        **kwargs
    ) -> None:
        """
        Load data from source DataFrame to target table.
        
        Args:
            source_df: Source DataFrame to load
            target_table: Target table name (can include database/schema)
            target_catalog: Catalog name (iceberg, clickhouse, etc.)
            **kwargs: Additional strategy-specific parameters
        """
        pass
    
    def _get_full_table_name(self, table: str, catalog: str) -> str:
        """Get full table name with catalog prefix."""
        return f"{catalog}.{table}" if catalog else table

