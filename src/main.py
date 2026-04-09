import sys
import json
import traceback
import os
from src.domain.pipeline.models import PipelineConfig
from src.infrastructure.connectors.spark_connector import SparkConnector
from src.infrastructure.loading_adapters.factory import StorageAdapterFactory
from src.application.pipeline.transformer import DynamicTransformer
from src.application.pipeline.job import JsonPipelineJob
from src.shared.logger import get_logger

logger = get_logger("Main")

def _initialize_spark(config: PipelineConfig):
    """Dynamically switch catalog initialization based on the pipeline needs."""
    if config.target.type == "iceberg" or config.source.type == "iceberg":
        logger.info("Initializing Spark Session with Iceberg Catalog")
        return SparkConnector.create_with_iceberg(app_name=config.pipeline_name)
    else:
        logger.info("Initializing Spark Session with ClickHouse Catalog")
        return SparkConnector.create_with_clickhouse(app_name=config.pipeline_name)

def main():
    if len(sys.argv) < 2:
        logger.error("Missing JSON Payload argument. Usage: spark-submit main.py <json_string_or_filepath>")
        sys.exit(1)
        
    payload_input = sys.argv[1]
    
    # 1. Parse Input
    try:
        # Check if the input is a file path (for local testing)
        if os.path.isfile(payload_input) and payload_input.endswith('.json'):
            logger.info(f"Reading JSON config from file: {payload_input}")
            with open(payload_input, 'r', encoding='utf-8') as f:
                json_payload = json.load(f)
        else:
            # Assume it's a raw JSON string (for K8s Spark Operator)
            logger.info("Parsing raw JSON string payload from arguments.")
            json_payload = json.loads(payload_input)
            
        pipeline_config = PipelineConfig(**json_payload)
        logger.info(f"Successfully Loaded Pipeline Config: {pipeline_config.pipeline_name}")
        
    except Exception as e:
        logger.error(f"Failed to parse Pipeline Configuration: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)

    # 2. Setup Services & Run
    spark = None
    try:
        spark = _initialize_spark(pipeline_config)
        data_port = StorageAdapterFactory.get_adapter(spark, pipeline_config.target.type)
        transformer = DynamicTransformer(pipeline_config.transform)
        
        job = JsonPipelineJob(
            config=pipeline_config,
            spark=spark,
            data_port=data_port,
            transformer=transformer
        )
        
        # 3. Execution
        logger.info("====== PIPELINE START ======")
        job.run()
        logger.info("====== PIPELINE COMPLETE ======")
        
    except Exception as e:
        logger.error(f"Pipeline Runtime Error: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)  # Ensure a non-zero exit code so K8s Operator can catch the failure
        
    finally:
        if spark is not None:
            spark.stop()

if __name__ == "__main__":
    main()
