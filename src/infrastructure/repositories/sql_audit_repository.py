from datetime import datetime
from typing import Optional
from sqlalchemy.orm import Session
from src.domain.audit.repository import IAuditRepository
from src.domain.audit.models import JobAudit
from src.infrastructure.database.models import JobAuditORM

class SqlAuditRepository(IAuditRepository):
    """Implementation of IAuditRepository using SQLAlchemy to log to Postgres/Oracle."""
    
    def __init__(self, session: Session):
        self.session = session

    def create_job(self, job_name: str, status: str = "RUNNING") -> JobAudit:
        """Create a new job execution record."""
        audit_orm = JobAuditORM(
            job_name=job_name,
            status=status,
            started_at=datetime.utcnow()
        )
        self.session.add(audit_orm)
        self.session.commit()
        self.session.refresh(audit_orm)
        
        return JobAudit.model_validate(audit_orm)
        
    def update_job(
        self, 
        job_id: int, 
        status: str, 
        total_records: int = 0, 
        error_message: Optional[str] = None
    ) -> JobAudit:
        """Update an existing job record."""
        audit_orm = self.session.query(JobAuditORM).filter(JobAuditORM.id == job_id).first()
        
        if audit_orm:
            audit_orm.status = status
            audit_orm.total_records = total_records
            audit_orm.error_message = error_message
            if status in ["SUCCESS", "FAILED"]:
                audit_orm.ended_at = datetime.utcnow()
                
            self.session.commit()
            self.session.refresh(audit_orm)
            return JobAudit.model_validate(audit_orm)
            
        raise ValueError(f"JobAudit with id {job_id} not found")
