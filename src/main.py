import sys
import json
import traceback
import os
import base64
import argparse
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
    parser = argparse.ArgumentParser(description="JsonPipelineJob Executable")
    parser.add_argument("--config-b64", type=str, help="Base64 encoded JSON configuration (Safe for Docker Exec)")
    parser.add_argument("payload", type=str, nargs='?', help="Raw JSON string or filepath (e.g. payload.json) for testing")
    
    args = parser.parse_args()
    
    if not args.config_b64 and not args.payload:
        logger.error("Missing Input. Usage: spark-submit src/main.py --config-b64 <base64> OR spark-submit src/main.py payload.json")
        sys.exit(1)
        
    # 1. Parse Input
    try:
        if args.config_b64:
            logger.info("Decoding Base64 JSON payload from arguments.")
            decoded_str = base64.b64decode(args.config_b64).decode('utf-8')
            json_payload = json.loads(decoded_str)
            
        elif args.payload:
            payload_input = args.payload
            
            # Check if the input is a file path (for local testing)
            if payload_input.endswith('.json'):
                if not os.path.isfile(payload_input):
                    raise FileNotFoundError(f"Config file not found at path: {payload_input}. Make sure to pass the absolute path in docker.")
                    
                logger.info(f"Reading JSON config from file: {payload_input}")
                with open(payload_input, 'r', encoding='utf-8') as f:
                    json_payload = json.load(f)
            else:
                # Assume it's a raw JSON string
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
