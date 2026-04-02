from abc import ABC, abstractmethod
from typing import List, Optional
from pyspark.sql import DataFrame
from datetime import datetime

class ILoadingDataPort(ABC):
    """
    Interface for Physical Data Loading. 
    Implemented by Infrastructure layer (e.g., IcebergAdapter, ClickHouseGenericAdapter).
    """

    @abstractmethod
    def execute_delete_insert(
        self,
        source_df: DataFrame,
        full_table_name: str,
        filter_condition: Optional[str] = None
    ) -> None:
        """Execute a full refresh or filtered replacement."""
        pass

    @abstractmethod
    def execute_merge_upsert(
        self,
        source_df: DataFrame,
        full_table_name: str,
        merge_keys: List[str]
    ) -> None:
        """Execute an incremental Merge/Upsert."""
        pass

    @abstractmethod
    def execute_scd_type2(
        self,
        source_df: DataFrame,
        full_table_name: str,
        business_keys: List[str],
        effective_date: Optional[datetime] = None
    ) -> None:
        """Execute Slowly Changing Dimension Type 2 updates."""
        pass
