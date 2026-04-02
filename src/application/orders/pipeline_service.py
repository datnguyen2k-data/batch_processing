from src.domain.orders.data_port import IOrdersDataPort
from src.domain.orders.transformations import OrderTransformer
from src.domain.audit.repository import IAuditRepository
from src.shared.logger import get_logger

logger = get_logger("FactOrderLinePipeline")

class FactOrderLinePipelineService:
    """Orchestrates the extraction, transformation and loading of fact_order_line."""
    
    def __init__(
        self,
        data_port: IOrdersDataPort,
        transformer: OrderTransformer,
        audit_repo: IAuditRepository
    ):
        self.data_port = data_port
        self.transformer = transformer
        self.audit_repo = audit_repo
        
    def run_pipeline(self) -> None:
        """Execute the Fact Order Line generation pipeline."""
        
        job = self.audit_repo.create_job(job_name="FactOrderLine_ETL")
        logger.info("FactOrderLine Pipeline Started", extra={"job_id": job.id})
        
        try:
            # 1. Instruct the Infrastructure to read Logical Plans from ClickHouse
            logger.info("Fetching raw data from ClickHouse", extra={"job_id": job.id})
            orders_df = self.data_port.read_orders()
            order_lines_df = self.data_port.read_order_lines()
            
            # 2. Pass DataFrames to Domain logic for transformation (Join Operation)
            logger.info("Applying Domain Transformation (Join)", extra={"job_id": job.id})
            fact_df = self.transformer.apply_join_rules(orders_df, order_lines_df)
            
            # 3. Instruct the Infrastructure to persist the result
            logger.info("Writing transformed data to ClickHouse", extra={"job_id": job.id})
            self.data_port.write_fact_order_line(fact_df)
            
            # 4. Success Completion (Note: Exact row count might be expensive to evaluate without caching 
            # if we wanted it, so we'll leave it as 0 unless explicitly needed)
            self.audit_repo.update_job(job.id, status="SUCCESS", total_records=0)
            logger.info("Pipeline Finished Successfully", extra={"job_id": job.id})
            
        except Exception as e:
            # 5. Capture Error State
            logger.error("Pipeline Failed", extra={"job_id": job.id}, exc_info=True)
            self.audit_repo.update_job(job.id, status="FAILED", error_message=str(e))
            raise e
