"""A module containing the Settings."""

from pydantic_settings import BaseSettings, SettingsConfigDict

from pathlib import Path

class Settings(BaseSettings):   
    azure_storage_connection_string: str
    azure_container: str
    spark_app_name: str
    log_level: str = "INFO"
    raw_dir: Path = Path("data/raw")
    delta_dir: Path = Path("data/delta")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )
    
settings = Settings()