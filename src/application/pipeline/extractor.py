from pyspark.sql import SparkSession, DataFrame
from src.domain.pipeline.models import SourceConfig
from src.shared.logger import get_logger

logger = get_logger("ExtractorFactory")

class ExtractorFactory:
    """Factory to automatically extract data based on SourceConfig."""
    
    @staticmethod
    def extract(spark: SparkSession, source_conf: SourceConfig) -> DataFrame:
        logger.info(f"Extracting data from {source_conf.type} source: {source_conf.table}")
        
        # When using configured catalogs
        if source_conf.type == "clickhouse":
            # Assuming clickhouse catalog is configured in spark session
            table_path = f"clickhouse.{source_conf.database}.{source_conf.table}"
            return spark.table(table_path)
            
        elif source_conf.type == "iceberg":
            # Assuming iceberg catalog is configured in spark session
            table_path = f"iceberg.{source_conf.database}.{source_conf.table}"
            return spark.table(table_path)
            
        elif source_conf.type == "postgres":
            # Postgres jdbc read would typically require credentials, 
            # we assume for now this might be handled via properties or explicit jdbc read.
            # Minimal implementation for generic jdbc fallback:
            raise NotImplementedError("Postgres extraction via jdbc requires URL and credentials logic")
            
        else:
            raise ValueError(f"Unknown source type: {source_conf.type}")
