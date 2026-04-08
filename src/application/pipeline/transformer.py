from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from src.domain.pipeline.models import TransformConfig
from src.shared.logger import get_logger

logger = get_logger("DynamicTransformer")

class DynamicTransformer:
    """Transforms a DataFrame based on JSON mappings and packet expressions."""
    
    def __init__(self, transform_conf: TransformConfig):
        self.config = transform_conf

    def transform(self, df: DataFrame) -> DataFrame:
        logger.info(f"Applying transformation with {len(self.config.column_mapping)} column rules")
        
        select_exprs = []
        
        for mapping in self.config.column_mapping:
            # Type safe default
            spark_type = "string" 
            if mapping.type == "numbers": 
                spark_type = "double"
            elif mapping.type == "datetime": 
                spark_type = "timestamp"
            elif mapping.type == "boolean":
                spark_type = "boolean"
                
            # Future-proofing packet expressions (like Tableau Prep calculated fields)
            if mapping.expression:
                logger.debug(f"Compiling expression for {mapping.target}: {mapping.expression}")
                # expr() parses actual SQL expressions
                col_expr = F.expr(mapping.expression).cast(spark_type).alias(mapping.target)
                select_exprs.append(col_expr)
            elif mapping.source:
                # Basic column rename & cast mapping
                col_expr = F.col(mapping.source).cast(spark_type).alias(mapping.target)
                select_exprs.append(col_expr)
            else:
                raise ValueError("Each column mapping must define either a 'source' or an 'expression'")
                
        return df.select(*select_exprs)
