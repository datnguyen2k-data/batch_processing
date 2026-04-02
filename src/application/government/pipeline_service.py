from pyspark.sql import SparkSession
from src.domain.government.repository import IGovernmentRepository
from src.domain.audit.repository import IAuditRepository
from src.shared.logger import get_logger

logger = get_logger("GovernmentPipeline")

class GovernmentPipelineService:
    """Application service coordinating the Control Plane (SQL) and Data Plane (Spark)."""
    
    def __init__(
        self, 
        government_repo: IGovernmentRepository, 
        audit_repo: IAuditRepository, 
        spark: SparkSession
    ):
        self.gov_repo = government_repo
        self.audit_repo = audit_repo
        self.spark = spark

    def run_pipeline(self):
        """Main execution flow for processing government data pipelines."""
        
        # 1. Start Job Audit (Observability)
        job = self.audit_repo.create_job("GovernmentDailySync")
        logger.info("Pipeline started", extra={"job_id": job.id, "job_name": job.job_name})
        
        total_processed_records = 0
        
        try:
            # 2. Fetch Control Configs (SQLAlchemy)
            configs = self.gov_repo.get_active_configs()
            
            if not configs:
                logger.warning("No active configurations found to process.", extra={"job_id": job.id})
                self.audit_repo.update_job(job.id, status="SUCCESS", total_records=0)
                return
            
            logger.info(f"Found {len(configs)} configuration(s) to process", extra={"job_id": job.id})
            
            # 3. Process each configuration (Data Plane - Spark)
            for config in configs:
                logger.info(f"Processing province: {config.province_id}", 
                            extra={"job_id": job.id, "config_id": config.id})
                
                # Mock Spark Data Read/Transform logic based on config rules
                # df = self.spark.read.parquet("s3a://data/raw/...")
                # df_filtered = df.filter(df.province_id == config.province_id)
                
                # if config.needs_anonymization:
                #    df_filtered = df_filtered.withColumn("name", mask_udf("name"))
                
                # df_filtered.writeTo(f"clickhouse.analytics.{config.target_table_name}").append()
                
                # Simulate record processing count
                records_batch = 100 
                total_processed_records += records_batch
                
                # Mark this config as processed
                self.gov_repo.mark_as_processed(config.id)
                
            # 4. Mark Job as Success
            self.audit_repo.update_job(job.id, status="SUCCESS", total_records=total_processed_records)
            logger.info("Pipeline finished successfully", 
                        extra={"job_id": job.id, "total_records": total_processed_records})
            
        except Exception as e:
            # 5. Mark Job as Failed with Error Traces
            logger.error("Pipeline failed", extra={"job_id": job.id}, exc_info=True)
            self.audit_repo.update_job(job.id, status="FAILED", error_message=str(e))
            raise
