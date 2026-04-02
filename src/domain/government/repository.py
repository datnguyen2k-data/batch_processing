from abc import ABC, abstractmethod
from typing import List
from .models import GovernmentConfig

class IGovernmentRepository(ABC):
    """Repository interface for fetching and managing government configuration rules."""
    
    @abstractmethod
    def get_active_configs(self) -> List[GovernmentConfig]:
        """Fetch all active configuration rules."""
        pass
        
    @abstractmethod
    def mark_as_processed(self, config_id: int) -> None:
        """Mark a configuration as processed with current timestamp."""
        pass
