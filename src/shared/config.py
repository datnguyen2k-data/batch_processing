"""Shared configuration module for loading environment variables."""
from dotenv import load_dotenv
import os
from typing import Optional

load_dotenv()


class SparkConfig:
    """Spark configuration from environment variables."""
    
    @staticmethod
    def get_app_name() -> str:
        return os.getenv("SPARK_APP_NAME", "batch-processing")
    
    @staticmethod
    def get_master() -> str:
        return os.getenv("SPARK_MASTER", "spark://spark-master:7077")


class MinIOConfig:
    """MinIO configuration for Iceberg."""
    
    @staticmethod
    def get_endpoint() -> str:
        return os.getenv("MINIO_ENDPOINT", "localhost:9000")
    
    @staticmethod
    def get_access_key() -> str:
        return os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    
    @staticmethod
    def get_secret_key() -> str:
        return os.getenv("MINIO_SECRET_KEY", "minioadmin")
    
    @staticmethod
    def get_use_ssl() -> bool:
        return os.getenv("MINIO_USE_SSL", "false").lower() == "true"
    
    @staticmethod
    def get_bucket() -> str:
        return os.getenv("MINIO_BUCKET", "iceberg-data")
    
    @staticmethod
    def get_warehouse_path() -> str:
        return os.getenv("MINIO_WAREHOUSE_PATH", "s3a://iceberg-data/warehouse")


class ClickHouseConfig:
    """ClickHouse configuration."""
    
    @staticmethod
    def get_host() -> str:
        return os.getenv("CLICKHOUSE_HOST", "localhost")
    
    @staticmethod
    def get_port() -> str:
        return os.getenv("CLICKHOUSE_PORT", "8123")
    
    @staticmethod
    def get_user() -> str:
        return os.getenv("CLICKHOUSE_USER", "default")
    
    @staticmethod
    def get_password() -> str:
        return os.getenv("CLICKHOUSE_PASSWORD", "")
    
    @staticmethod
    def get_database() -> str:
        return os.getenv("CLICKHOUSE_DB", "default")

