from sqlalchemy import Column, Integer, String, Boolean, DateTime, Text
from datetime import datetime
from src.infrastructure.database.sql_connector import Base

class GovernmentConfigORM(Base):
    """SQLAlchemy model mapping to the government_configs table."""
    __tablename__ = "government_configs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    province_id = Column(String(50), nullable=False)
    target_table_name = Column(String(255), nullable=False)
    needs_anonymization = Column(Boolean, default=False)
    is_active = Column(Boolean, default=True)
    last_processed_at = Column(DateTime, nullable=True)


class JobAuditORM(Base):
    """SQLAlchemy model mapping to the job_audits table."""
    __tablename__ = "job_audits"

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_name = Column(String(255), nullable=False)
    status = Column(String(50), nullable=False) # e.g. RUNNING, SUCCESS, FAILED
    total_records = Column(Integer, default=0)
    error_message = Column(Text, nullable=True)
    started_at = Column(DateTime, default=datetime.utcnow)
    ended_at = Column(DateTime, nullable=True)
