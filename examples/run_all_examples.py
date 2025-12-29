"""
Main script to run all examples.
"""
from src.infrastructure.connectors.spark_connector import SparkConnector
from examples.loading_examples import (
    example_delete_insert,
    example_merge_upsert,
    example_scd_type2,
    example_clickhouse_loading
)
from examples.join_examples import (
    example_basic_joins,
    example_advanced_joins,
    example_broadcast_join,
    example_bucket_join,
    example_complex_join_scenario
)
from src.application.batch_processor import BatchProcessor


def main():
    """Run all examples."""
    print("=" * 60)
    print("Batch Processing Examples - Spark with DDD Architecture")
    print("=" * 60)
    
    # Create Spark session with all catalogs
    spark = SparkConnector.create_with_all_catalogs("BatchProcessingExamples")
    processor = BatchProcessor(spark)
    
    try:
        # Run loading examples
        print("\n" + "=" * 60)
        print("LOADING STRATEGIES EXAMPLES")
        print("=" * 60)
        
        example_delete_insert(spark, processor)
        example_merge_upsert(spark, processor)
        example_scd_type2(spark, processor)
        example_clickhouse_loading(spark, processor)
        
        # Run join examples
        print("\n" + "=" * 60)
        print("JOIN STRATEGIES EXAMPLES")
        print("=" * 60)
        
        example_basic_joins(spark)
        example_advanced_joins(spark)
        example_broadcast_join(spark)
        example_bucket_join(spark)
        example_complex_join_scenario(spark)
        
        print("\n" + "=" * 60)
        print("All examples completed successfully!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\nError running examples: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()
        print("\nSpark session stopped.")


if __name__ == "__main__":
    main()

