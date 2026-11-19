from functools import lru_cache

from pydantic import Field
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

    redis_url: str

    rate_limit_capacity: int = Field(gt=0)
    rate_limit_refill_per_second: float = Field(gt=0)

    provider_timeout_seconds: float = Field(gt=0)
    provider_a_concurrency: int = Field(gt=0)
    provider_b_concurrency: int = Field(gt=0)
    provider_c_concurrency: int = Field(gt=0)
    provider_failure_threshold: int = Field(gt=0)
    provider_cooldown_seconds: float = Field(gt=0)

    max_inflight_jobs: int = Field(gt=0)
    inflight_job_ttl_seconds: int = Field(gt=0)
    max_provider_waits: int = Field(gt=0)

    mongodb_url: str
    mongodb_db_name: str
    mongodb_retention_seconds: int = Field(gt=0)


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
