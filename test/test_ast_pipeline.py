import json
import traceback
from src.domain.pipeline.models import PipelineConfig
from src.infrastructure.connectors.spark_connector import SparkConnector
from src.infrastructure.loading_adapters.factory import StorageAdapterFactory
from src.application.pipeline.transformer import DynamicTransformer
from src.application.pipeline.job import JsonPipelineJob
from src.shared.logger import get_logger

logger = get_logger("AST_Test")

def main():
    logger.info("Initializing Spark Session for AST validation")
    
    # Generate an AST payload simulating output from TypeScript Control Plane
    # This AST corresponds to:
    # 1. order_id: Direct mapping
    # 2. total_amount: IIF([status] = 'OK', [total_amount] * 1.1, [total_amount]) -> mapped to F.when
    
    json_payload = {
        "run_id": "job_101_ts_to_python",
        "control_plane_url": "http://localhost:3000",
        "pipeline_name": "ast_order_transformer",
        "source": {
            "type": "clickhouse",
            "database": "ldz",
            "table": "orders"
        },
        "target": {
            "type": "clickhouse",
            "database": "hmz",
            "table": "orders",
            "write_mode": "upsert",
            "primary_keys": ["order_id"]
        },
        "transform": {
            "column_mapping": [
                {
                    "source": "order_id",
                    "target": "order_id",
                    "type": "long"
                },
                {
                    "target": "total_amount",
                    "type": "double",
                    "ast": {
                        "type": "FunctionCall",
                        "name": "IIF",
                        "args": [
                            {
                                "type": "BinaryOp",
                                "op": "=",
                                "left": {"type": "ColumnRef", "name": "status"},
                                "right": {"type": "String", "value": "OK"}
                            },
                            {
                                "type": "BinaryOp",
                                "op": "*",
                                "left": {"type": "ColumnRef", "name": "total_amount"},
                                "right": {"type": "Number", "value": 1.1}
                            },
                            {"type": "ColumnRef", "name": "total_amount"}
                        ]
                    }
                }
            ]
        }
    }
    
    # 1. Validation Logic
    try:
        pipeline_config = PipelineConfig(**json_payload)
        logger.info("Parsed AST Configuration Successfully.")
    except Exception as e:
        logger.error(f"Failed to parse model: {e}")
        return

    # 2. Start Services
    try:
        spark = SparkConnector.create_with_clickhouse(app_name=pipeline_config.pipeline_name)
        data_port = StorageAdapterFactory.get_adapter(spark, pipeline_config.target.type)
        transformer = DynamicTransformer(pipeline_config.transform)
        
        job = JsonPipelineJob(
            config=pipeline_config,
            spark=spark,
            data_port=data_port,
            transformer=transformer
        )
        
        # 3. Execution
        job.run()
        
    except Exception as e:
        logger.error(f"Job failed: {e}")
        logger.error(traceback.format_exc())
    finally:
        if 'spark' in locals() and spark is not None:
            spark.stop()

if __name__ == "__main__":
    main()
