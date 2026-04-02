from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class JobAudit(BaseModel):
    """Business model representing a logged audit trace for a data pipeline job."""
    id: Optional[int] = None
    job_name: str
    status: str
    total_records: int = 0
    error_message: Optional[str] = None
    started_at: Optional[datetime] = None
    ended_at: Optional[datetime] = None
    
    class Config:
        from_attributes = True
