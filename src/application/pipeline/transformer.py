from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from typing import List
from src.domain.pipeline.models import ColumnMapping, TransformConfig
from src.application.pipeline.ast_visitor import SparkAstVisitor
from src.shared.logger import get_logger

logger = get_logger("DynamicTransformer")

class DynamicTransformer:
    """Transforms a DataFrame based on JSON mappings and packet expressions."""
    
    def __init__(self, transform_conf: TransformConfig):
        self.config = transform_conf
        self.ast_visitor = SparkAstVisitor()

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
            # Priority 1: AST payload from Control Plane
            if mapping.ast:
                col_expr = self.ast_visitor.visit(mapping.ast).cast(spark_type).alias(mapping.target)
                select_exprs.append(col_expr)
            # Priority 2: Legacy raw SQL string expression (if AST is missing)
            elif mapping.expression:
                logger.debug(f"Compiling expression for {mapping.target}: {mapping.expression}")
                # expr() parses actual SQL expressions
                col_expr = F.expr(mapping.expression).cast(spark_type).alias(mapping.target)
                select_exprs.append(col_expr)
            # Priority 3: Direct Column mapping
            elif mapping.source:
                # Basic column rename & cast mapping
                col_expr = F.col(mapping.source).cast(spark_type).alias(mapping.target)
                select_exprs.append(col_expr)
            else:
                raise ValueError("Each column mapping must define either a 'source' or an 'expression'")
                
        return df.select(*select_exprs)
