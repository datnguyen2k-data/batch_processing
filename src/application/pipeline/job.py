from pyspark.sql import SparkSession
from src.domain.pipeline.models import PipelineConfig
from src.domain.loading.data_port import ILoadingDataPort
from src.infrastructure.connectors.spark_connector import SparkConnector
from src.infrastructure.loading_adapters.factory import StorageAdapterFactory
from src.application.pipeline.extractor import ExtractorFactory
from src.application.pipeline.transformer import DynamicTransformer
from src.shared.logger import get_logger

logger = get_logger("JsonPipelineJob")

class JsonPipelineJob:
    """A Spark Job generated dynamically from a JSON configuration."""
    
    def __init__(
        self, 
        config: PipelineConfig,
        spark: SparkSession,
        data_port: ILoadingDataPort,
        transformer: DynamicTransformer,
    ):
        self.config = config
        self.spark = spark
        self.data_port = data_port
        self.transformer = transformer
        
    def run(self):
        logger.info(f"Starting auto-extraction for generic source: {self.config.source.table}")
        source_df = ExtractorFactory.extract(self.spark, self.config.source)
        
        logger.info("Applying dynamic transformations")
        transformed_df = self.transformer.transform(source_df)
        
        target = self.config.target
        
        # Prepend the catalog namespace based on the target type
        catalog_prefix = "clickhouse" if target.type == "clickhouse" else target.type
        full_table_name = f"{catalog_prefix}.{target.database}.{target.table}"
        
        logger.info(f"Executing write mode '{target.write_mode}' to {full_table_name}")
        if target.write_mode == "upsert":
            if not target.primary_keys:
                raise ValueError("upsert mode requires target.primary_keys to be defined")
            self.data_port.execute_merge_upsert(transformed_df, full_table_name, target.primary_keys)
            
        elif target.write_mode == "delete_insert":
            self.data_port.execute_delete_insert(transformed_df, full_table_name)
            
        elif target.write_mode == "scd_type2":
            if not target.primary_keys:
                raise ValueError("scd_type2 mode requires target.primary_keys to be defined")
            self.data_port.execute_scd_type2(transformed_df, full_table_name, target.primary_keys)
            
        else:
            raise ValueError(f"Unsupported write mode: {target.write_mode}")
            
        logger.info(f"Pipeline job {self.config.pipeline_name} completed successfully")
