from pydantic_settings import BaseSettings
from pydantic import Field
from typing import Optional

class Settings(BaseSettings):
    """Application settings with environment variable support."""
    
    # API Configuration
    etoro_api_key: str = Field(..., validation_alias='ETORO_API_KEY')
    etoro_user_key: str = Field(..., validation_alias='ETORO_USER_KEY')
    etoro_base_url: str = Field("https://public-api.etoro.com", validation_alias='ETORO_BASE_URL')
    etoro_username: Optional[str] = Field(None, validation_alias='ETORO_USERNAME')
    
    # Application Settings
    data_dir: str = Field("data", validation_alias='DATA_DIR')
    log_dir: str = Field("logs", validation_alias='LOG_DIR')
    
    # Trade Fetching Settings
    default_days_back: int = Field(30, validation_alias='DEFAULT_DAYS_BACK')
    max_retries: int = Field(3, validation_alias='MAX_RETRIES')
    timeout_seconds: int = Field(30, validation_alias='TIMEOUT_SECONDS')
    
    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "case_sensitive": False,
        "extra": "ignore"
    }

# Global settings instance
settings = Settings()