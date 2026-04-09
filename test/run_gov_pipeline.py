from src.infrastructure.database.sql_connector import SqlConnector, Base
from src.infrastructure.connectors.spark_connector import SparkConnector
from src.infrastructure.repositories.sql_government_repository import SqlGovernmentRepository
from src.infrastructure.repositories.sql_audit_repository import SqlAuditRepository
from src.application.government.pipeline_service import GovernmentPipelineService

def main():
    # 0. [Optional for Dev] Create tables if they don't exist (in prod use alembic migrations)
    engine = SqlConnector.get_engine()
    Base.metadata.create_all(bind=engine)
    
    # 1. Initialize Dependency Injection: SQL Session (Control Plane)
    db_session = SqlConnector.get_session_factory()()
    gov_repo = SqlGovernmentRepository(db_session)
    audit_repo = SqlAuditRepository(db_session)
    
    # 2. Initialize Dependency Injection: Spark Session (Data Plane)
    spark = SparkConnector.create_with_clickhouse(app_name="Government_Batch_Pipeline")
    
    # 3. Initialize the Application Service
    pipeline = GovernmentPipelineService(
        government_repo=gov_repo,
        audit_repo=audit_repo,
        spark=spark
    )
    
    try:
        # 4. Execute Workflow
        pipeline.run_pipeline()
    finally:
        # 5. Clean up
        db_session.close()
        spark.stop()

if __name__ == "__main__":
    main()
