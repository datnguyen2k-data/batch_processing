from abc import ABC, abstractmethod
from typing import Optional
from .models import JobAudit

class IAuditRepository(ABC):
    """Repository interface for logging pipeline job audits."""
    
    @abstractmethod
    def create_job(self, job_name: str, status: str = "RUNNING") -> JobAudit:
        """Create a new job execution record."""
        pass
        
    @abstractmethod
    def update_job(
        self, 
        job_id: int, 
        status: str, 
        total_records: int = 0, 
        error_message: Optional[str] = None
    ) -> JobAudit:
        """Update an existing job record, e.g. marking it as SUCCESS or FAILED."""
        pass

