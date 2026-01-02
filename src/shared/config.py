"""Shared configuration module using Pydantic v2 for loading environment variables."""
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
from typing import Optional


class SparkSettings(BaseSettings):
    """Spark configuration from environment variables."""
    
    app_name: str = Field(default="batch-processing", alias="SPARK_APP_NAME")
    master: str = Field(default="spark://spark-master:7077", alias="SPARK_MASTER")
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )


class MinIOSettings(BaseSettings):
    """MinIO configuration for Iceberg."""
    
    endpoint: str = Field(default="localhost:9000", alias="MINIO_ENDPOINT")
    access_key: str = Field(default="minioadmin", alias="MINIO_ACCESS_KEY")
    secret_key: str = Field(default="minioadmin", alias="MINIO_SECRET_KEY")
    use_ssl: bool = Field(default=False, alias="MINIO_USE_SSL")
    bucket: str = Field(default="iceberg-data", alias="MINIO_BUCKET")
    warehouse_path: str = Field(
        default="s3a://iceberg-data/warehouse",
        alias="MINIO_WAREHOUSE_PATH"
    )
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )


class ClickHouseSettings(BaseSettings):
    """ClickHouse configuration."""
    
    host: str = Field(default="localhost", alias="CLICKHOUSE_HOST")
    port: str = Field(default="8123", alias="CLICKHOUSE_PORT")
    user: str = Field(default="default", alias="CLICKHOUSE_USER")
    password: str = Field(default="", alias="CLICKHOUSE_PASSWORD")
    database: str = Field(default="default", alias="CLICKHOUSE_DB")
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )


# Singleton instances
_spark_settings: Optional[SparkSettings] = None
_minio_settings: Optional[MinIOSettings] = None
_clickhouse_settings: Optional[ClickHouseSettings] = None


def _get_spark_settings() -> SparkSettings:
    """Get or create SparkSettings singleton instance."""
    global _spark_settings
    if _spark_settings is None:
        _spark_settings = SparkSettings()
    return _spark_settings


def _get_minio_settings() -> MinIOSettings:
    """Get or create MinIOSettings singleton instance."""
    global _minio_settings
    if _minio_settings is None:
        _minio_settings = MinIOSettings()
    return _minio_settings


def _get_clickhouse_settings() -> ClickHouseSettings:
    """Get or create ClickHouseSettings singleton instance."""
    global _clickhouse_settings
    if _clickhouse_settings is None:
        _clickhouse_settings = ClickHouseSettings()
    return _clickhouse_settings


# Backward compatibility: Static classes that match the original API
class SparkConfig:
    """Spark configuration from environment variables (backward compatible)."""
    
    @staticmethod
    def get_app_name() -> str:
        return _get_spark_settings().app_name
    
    @staticmethod
    def get_master() -> str:
        return _get_spark_settings().master


class MinIOConfig:
    """MinIO configuration for Iceberg (backward compatible)."""
    
    @staticmethod
    def get_endpoint() -> str:
        return _get_minio_settings().endpoint
    
    @staticmethod
    def get_access_key() -> str:
        return _get_minio_settings().access_key
    
    @staticmethod
    def get_secret_key() -> str:
        return _get_minio_settings().secret_key
    
    @staticmethod
    def get_use_ssl() -> bool:
        return _get_minio_settings().use_ssl
    
    @staticmethod
    def get_bucket() -> str:
        return _get_minio_settings().bucket
    
    @staticmethod
    def get_warehouse_path() -> str:
        return _get_minio_settings().warehouse_path


class ClickHouseConfig:
    """ClickHouse configuration (backward compatible)."""
    
    @staticmethod
    def get_host() -> str:
        return _get_clickhouse_settings().host
    
    @staticmethod
    def get_port() -> str:
        return _get_clickhouse_settings().port
    
    @staticmethod
    def get_user() -> str:
        return _get_clickhouse_settings().user
    
    @staticmethod
    def get_password() -> str:
        return _get_clickhouse_settings().password
    
    @staticmethod
    def get_database() -> str:
        return _get_clickhouse_settings().database
