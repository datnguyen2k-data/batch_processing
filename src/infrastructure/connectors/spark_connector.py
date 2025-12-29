"""Spark connector factory for creating Spark sessions with different configurations."""
from pyspark.sql import SparkSession
from typing import Optional
from src.shared.config import SparkConfig, MinIOConfig, ClickHouseConfig


class SparkConnector:
    """Factory class for creating Spark sessions with different catalog configurations."""
    
    @staticmethod
    def create_base_session(
        app_name: Optional[str] = None,
        master: Optional[str] = None
    ) -> SparkSession:
        """Create a base Spark session."""
        app_name = app_name or SparkConfig.get_app_name()
        master = master or SparkConfig.get_master()
        
        spark = SparkSession.builder \
            .appName(app_name) \
            .master(master) \
            .getOrCreate()
        
        return spark
    
    @staticmethod
    def create_with_iceberg(
        app_name: Optional[str] = None,
        master: Optional[str] = None
    ) -> SparkSession:
        """Create Spark session with Iceberg catalog configured for MinIO."""
        spark = SparkConnector.create_base_session(app_name, master)
        
        # Configure S3A (MinIO) for Iceberg
        spark.conf.set("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        spark.conf.set("spark.sql.catalog.iceberg.type", "hadoop")
        spark.conf.set("spark.sql.catalog.iceberg.warehouse", MinIOConfig.get_warehouse_path())
        
        # MinIO S3A configuration
        spark.conf.set("spark.hadoop.fs.s3a.endpoint", MinIOConfig.get_endpoint())
        spark.conf.set("spark.hadoop.fs.s3a.access.key", MinIOConfig.get_access_key())
        spark.conf.set("spark.hadoop.fs.s3a.secret.key", MinIOConfig.get_secret_key())
        spark.conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
        spark.conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        spark.conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", str(MinIOConfig.get_use_ssl()))
        
        # Iceberg specific configurations
        spark.conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        spark.conf.set("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        
        return spark
    
    @staticmethod
    def create_with_clickhouse(
        app_name: Optional[str] = None,
        master: Optional[str] = None
    ) -> SparkSession:
        """Create Spark session with ClickHouse catalog."""
        spark = SparkConnector.create_base_session(app_name, master)
        
        # Configure ClickHouse catalog
        spark.conf.set("spark.sql.catalog.clickhouse", "com.clickhouse.spark.ClickHouseCatalog")
        spark.conf.set("spark.sql.catalog.clickhouse.host", ClickHouseConfig.get_host())
        spark.conf.set("spark.sql.catalog.clickhouse.protocol", "http")
        spark.conf.set("spark.sql.catalog.clickhouse.http_port", ClickHouseConfig.get_port())
        spark.conf.set("spark.sql.catalog.clickhouse.user", ClickHouseConfig.get_user())
        spark.conf.set("spark.sql.catalog.clickhouse.password", ClickHouseConfig.get_password())
        spark.conf.set("spark.sql.catalog.clickhouse.database", ClickHouseConfig.get_database())
        
        return spark
    
    @staticmethod
    def create_with_all_catalogs(
        app_name: Optional[str] = None,
        master: Optional[str] = None
    ) -> SparkSession:
        """Create Spark session with both Iceberg and ClickHouse catalogs."""
        spark = SparkConnector.create_with_iceberg(app_name, master)
        
        # Add ClickHouse catalog
        spark.conf.set("spark.sql.catalog.clickhouse", "com.clickhouse.spark.ClickHouseCatalog")
        spark.conf.set("spark.sql.catalog.clickhouse.host", ClickHouseConfig.get_host())
        spark.conf.set("spark.sql.catalog.clickhouse.protocol", "http")
        spark.conf.set("spark.sql.catalog.clickhouse.http_port", ClickHouseConfig.get_port())
        spark.conf.set("spark.sql.catalog.clickhouse.user", ClickHouseConfig.get_user())
        spark.conf.set("spark.sql.catalog.clickhouse.password", ClickHouseConfig.get_password())
        spark.conf.set("spark.sql.catalog.clickhouse.database", ClickHouseConfig.get_database())
        
        return spark

