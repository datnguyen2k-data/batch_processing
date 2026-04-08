import os
import sys
import json

# Add root project path to PYTHONPATH
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession
from unittest.mock import MagicMock

from src.infrastructure.connectors.spark_connector import SparkConnector
from src.domain.pipeline.models import PipelineConfig
from src.application.pipeline.extractor import ExtractorFactory
from src.application.pipeline.transformer import DynamicTransformer
from src.infrastructure.loading_adapters.factory import StorageAdapterFactory
from src.application.pipeline.job import JsonPipelineJob
from src.shared.logger import get_logger

logger = get_logger("MockTest")

def main():
    json_config = """
    {
      "pipeline_name": "order_profile_etl",
      "source": {
        "type": "clickhouse",
        "database": "ldz",
        "table": "orders"
      },
      "transform": {
        "column_mapping": [
          { "source": "order_id", "target": "order_id", "type": "numbers" },
          { "source": "customer_id", "target": "customer_id", "type": "numbers" },
          { "source": "order_date", "target": "order_date", "type": "datetime" },
          { "source": "status", "target": "status", "type": "string" },
          { "source": "total_amount", "target": "total_amount", "type": "numbers" },
          { "source": "version", "target": "version", "type": "numbers" }
        ]
      },
      "target": {
        "type": "clickhouse",
        "database": "hmz",
        "table": "orders",
        "write_mode": "upsert",
        "primary_keys": ["order_id"],
        "engine": "ReplacingMergeTree",
        "order_by": ["order_id"]
      }
    }
    """
    
    # 1. Parse Config
    logger.info("Parsing JSON Config...")
    config_dict = json.loads(json_config)
    pipeline_config = PipelineConfig(**config_dict)
    logger.info(f"Loaded config for pipeline: {pipeline_config.pipeline_name}")
    
    # 2. Setup Data Plane
    logger.info("Initializing Spark Session with Clickhouse Catalog...")
    # Using explicit pipeline name here
    spark = SparkConnector.create_with_clickhouse(app_name=pipeline_config.pipeline_name)
    
    # In real app, the target target_type would dynamically choose this via StorageAdapterFactory
    data_port = StorageAdapterFactory.get_adapter(spark, pipeline_config.target.type)
    transformer = DynamicTransformer(pipeline_config.transform)
    
    # 3. Inject Dependencies 
    logger.info("Initializing Pipeline Job Service...")
    job = JsonPipelineJob(
        config=pipeline_config,
        spark=spark,
        data_port=data_port,
        transformer=transformer
    )
    
    try:
        # 4. Run the actual job
        logger.info("Triggering Execution...")
        job.run()
        logger.info("✅ SUCCESS: The pipeline executed successfully!")
    finally:
        # 5. Clean Resource Closures
        logger.info("Stopping Spark Session...")
        spark.stop()

if __name__ == '__main__':
    main()
