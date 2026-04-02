from typing import List
from datetime import datetime
from sqlalchemy.orm import Session
from src.domain.government.repository import IGovernmentRepository
from src.domain.government.models import GovernmentConfig
from src.infrastructure.database.models import GovernmentConfigORM

class SqlGovernmentRepository(IGovernmentRepository):
    """Implementation of IGovernmentRepository using SQLAlchemy."""
    
    def __init__(self, session: Session):
        self.session = session
        
    def get_active_configs(self) -> List[GovernmentConfig]:
        """Fetch all active configuration rules from database."""
        configs_orm = self.session.query(GovernmentConfigORM).filter(
            GovernmentConfigORM.is_active == True
        ).all()
        
        # Parse ORM models to Pydantic Domain models
        return [GovernmentConfig.model_validate(config) for config in configs_orm]
        
    def mark_as_processed(self, config_id: int) -> None:
        """Mark a configuration as processed with current timestamp."""
        config_orm = self.session.query(GovernmentConfigORM).filter(
            GovernmentConfigORM.id == config_id
        ).first()
        
        if config_orm:
            config_orm.last_processed_at = datetime.utcnow()
            self.session.commit()
