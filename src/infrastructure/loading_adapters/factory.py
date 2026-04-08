from pyspark.sql import SparkSession
from src.domain.loading.data_port import ILoadingDataPort
from src.infrastructure.loading_adapters.iceberg_adapter import IcebergLoadingAdapter
from src.infrastructure.loading_adapters.generic_spark_adapter import GenericSparkLoadingAdapter
from src.infrastructure.loading_adapters.clickhouse_adapter import ClickHouseLoadingAdapter

class StorageAdapterFactory:
    """Factory to resolve the correct physical loading adapter depending on the target storage/catalog."""
    
    @staticmethod
    def get_adapter(spark: SparkSession, catalog_name: str) -> ILoadingDataPort:
        if catalog_name.lower() == "iceberg":
            return IcebergLoadingAdapter(spark)
        elif catalog_name.lower() == "clickhouse":
            return ClickHouseLoadingAdapter(spark)
        else:
            return GenericSparkLoadingAdapter(spark)
