from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

class IOrdersDataPort(ABC):
    """Port interface for data IO in the Orders Domain."""
    
    @abstractmethod
    def read_orders(self) -> DataFrame:
        """Fetch the orders table data."""
        pass
        
    @abstractmethod
    def read_order_lines(self) -> DataFrame:
        """Fetch the order_line table data."""
        pass
        
    @abstractmethod
    def write_fact_order_line(self, df: DataFrame) -> None:
        """Write the transformed DataFrame to fact_order_line."""
        pass
