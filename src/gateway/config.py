from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    app_name: str
    app_env: str
    log_level: str

    api_v1_prefix: str

    upstream_timeout_seconds: float
    upstream_max_connections: int
    upstream_max_keepalive_connections: int

    provider_a_url: str
    provider_b_url: str
    provider_c_url: str


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
