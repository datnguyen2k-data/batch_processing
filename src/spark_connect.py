"""Legacy connector functions - consider using SparkConnector from infrastructure.connectors instead."""
from pyspark.sql import SparkSession
from src.shared.config import ClickHouseConfig


def get_spark_clickhouse(app_name="SparkClickHouseApp"):
    """Create Spark session with ClickHouse catalog (legacy function)."""
    spark = SparkSession.builder.appName(app_name).getOrCreate()

    spark.conf.set("spark.sql.catalog.clickhouse", "com.clickhouse.spark.ClickHouseCatalog")
    spark.conf.set("spark.sql.catalog.clickhouse.host", ClickHouseConfig.get_host())
    spark.conf.set("spark.sql.catalog.clickhouse.protocol", "http")
    spark.conf.set("spark.sql.catalog.clickhouse.http_port", ClickHouseConfig.get_port())
    spark.conf.set("spark.sql.catalog.clickhouse.user", ClickHouseConfig.get_user())
    spark.conf.set("spark.sql.catalog.clickhouse.password", ClickHouseConfig.get_password())
    spark.conf.set("spark.sql.catalog.clickhouse.database", ClickHouseConfig.get_database())

    return spark

def kafka_connector(app_name="SparkKafkaApp"):
    """Create Spark session with Kafka catalog (legacy function)."""
    from pydantic_settings import BaseSettings, SettingsConfigDict
    from pydantic import Field
    
    class KafkaConfig(BaseSettings):
        """Kafka configuration."""
        bootstrap_servers: str = Field(default="", alias="KAFKA_BOOTSTRAP_SERVERS")
        security_protocol: str = Field(default="SASL_PLAINTEXT", alias="KAFKA_SECURITY_PROTOCOL")
        sasl_jaas_config: str = Field(default="", alias="KAFKA_JAAS_CONFIG")
        sasl_mechanisms: str = Field(default="", alias="KAFKA_SASL_MECHANISMS")
        ssl_truststore_location: str = Field(default="", alias="KAFKA_SSL_TRUSTSTORE_LOCATION")
        ssl_truststore_password: str = Field(default="", alias="KAFKA_SSL_TRUSTSTORE_PASSWORD")
        ssl_truststore_type: str = Field(default="", alias="KAFKA_SSL_TRUSTSTORE_TYPE")
        ssl_keystore_location: str = Field(default="", alias="KAFKA_SSL_KEYSTORE_LOCATION")
        ssl_keystore_password: str = Field(default="", alias="KAFKA_SSL_KEYSTORE_PASSWORD")
        ssl_keystore_type: str = Field(default="", alias="KAFKA_SSL_KEYSTORE_TYPE")
        ssl_key_password: str = Field(default="", alias="KAFKA_SSL_KEY_PASSWORD")
        
        model_config = SettingsConfigDict(
            env_file=".env",
            env_file_encoding="utf-8",
            case_sensitive=False,
            extra="ignore"
        )
    
    kafka_config = KafkaConfig()
    spark = SparkSession.builder.appName(app_name).getOrCreate()

    spark.conf.set("spark.sql.catalog.kafka", "org.apache.spark.sql.kafka.v2.KafkaCatalog")
    spark.conf.set("spark.sql.catalog.kafka.bootstrap.servers", kafka_config.bootstrap_servers)
    spark.conf.set("spark.sql.catalog.kafka.security.protocol", kafka_config.security_protocol)
    spark.conf.set("spark.sql.catalog.kafka.sasl.jaas.config", kafka_config.sasl_jaas_config)
    spark.conf.set("spark.sql.catalog.kafka.sasl.mechanisms", kafka_config.sasl_mechanisms)
    spark.conf.set("spark.sql.catalog.kafka.ssl.endpoint.identification.algorithm", "https")
    spark.conf.set("spark.sql.catalog.kafka.ssl.truststore.location", kafka_config.ssl_truststore_location)
    spark.conf.set("spark.sql.catalog.kafka.ssl.truststore.password", kafka_config.ssl_truststore_password)
    spark.conf.set("spark.sql.catalog.kafka.ssl.truststore.type", kafka_config.ssl_truststore_type)
    spark.conf.set("spark.sql.catalog.kafka.ssl.keystore.location", kafka_config.ssl_keystore_location)
    spark.conf.set("spark.sql.catalog.kafka.ssl.keystore.password", kafka_config.ssl_keystore_password)
    spark.conf.set("spark.sql.catalog.kafka.ssl.keystore.type", kafka_config.ssl_keystore_type)
    spark.conf.set("spark.sql.catalog.kafka.ssl.key.password", kafka_config.ssl_key_password)
    return spark

