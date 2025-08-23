from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    database_url: str = "postgresql+asyncpg://user:password@localhost/compute_db"

    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "receipts"
    kafka_group_id: str = "compute_service"

    spark_master: str = "local[*]"
    spark_app_name: str = "ReceiptAnalytics"

    redis_url: str = "redis://localhost:6379"

    api_host: str = "0.0.0.0"
    api_port: int = 8000

    log_level: str = "INFO"
    
    class Config:
        env_file = ".env"

settings = Settings()
