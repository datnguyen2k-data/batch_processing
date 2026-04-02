from pyspark.sql import DataFrame

class OrderTransformer:
    """Contains pure business logic transformations for the Orders Domain."""
    
    def apply_join_rules(self, orders_df: DataFrame, order_lines_df: DataFrame) -> DataFrame:
        """
        Join orders and order_lines and select specific fields to construct fact_order_line.
        
        Selection Schema:
        ol.order_id, ol.line_id, ol.price, ol.product_id, ol.quantity, o.status, o.order_date
        """
        
        # Alias dataframes for clean column resolution
        o = orders_df.alias("o")
        ol = order_lines_df.alias("ol")
        
        # We assume Inner Join is standard for fact tables without trailing orphans.
        joined_df = ol.join(o, on="order_id", how="inner")
        
        # Select exactly the columns required by business logic
        fact_df = joined_df.select(
            "ol.order_id",
            "ol.line_id",
            "ol.price",
            "ol.product_id",
            "ol.quantity",
            "o.status",
            "o.order_date"
        )
        
        return fact_df
