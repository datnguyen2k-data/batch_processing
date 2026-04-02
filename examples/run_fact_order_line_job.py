from src.infrastructure.database.sql_connector import SqlConnector, Base
from src.infrastructure.connectors.spark_connector import SparkConnector
from src.infrastructure.repositories.spark_orders_data_repo import SparkOrdersDataRepo
from src.infrastructure.repositories.sql_audit_repository import SqlAuditRepository
from src.domain.orders.transformations import OrderTransformer
from src.application.orders.pipeline_service import FactOrderLinePipelineService

def main():
    # 1. Setup Audit Logs Control Plane
    engine = SqlConnector.get_engine()
    Base.metadata.create_all(bind=engine)
    db_session = SqlConnector.get_session_factory()()
    audit_repo = SqlAuditRepository(db_session)
    
    # 2. Setup Data Plane via Spark ClickHouse connector
    spark = SparkConnector.create_with_clickhouse(app_name="FactOrderLine_ETL_Job")
    data_port = SparkOrdersDataRepo(spark)
    transformer = OrderTransformer()
    
    # 3. Inject Dependencies into Application Service
    pipeline = FactOrderLinePipelineService(
        data_port=data_port,
        transformer=transformer,
        audit_repo=audit_repo
    )
    
    try:
        # 4. Trigger Execution
        pipeline.run_pipeline()
    finally:
        # 5. Clean Resource Closures
        db_session.close()
        spark.stop()

if __name__ == "__main__":
    main()
