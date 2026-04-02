from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class GovernmentConfig(BaseModel):
    """Business model representing a configuration rule for processing government data."""
    id: int
    province_id: str
    target_table_name: str
    needs_anonymization: bool
    is_active: bool
    last_processed_at: Optional[datetime] = None
    
    class Config:
        from_attributes = True  # Allows parsing from SQLAlchemy ORM models
