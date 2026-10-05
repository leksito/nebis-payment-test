from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://payments:payments@localhost:5432/payments"
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"

    # Outbox relay
    outbox_poll_interval: float = Field(default=0.5, gt=0)
    outbox_batch_size: int = Field(default=100, gt=0)

    # Consumer: total delivery attempts before a message goes to the DLQ,
    # delays between attempts are base * 2 ** (attempt - 1)
    max_attempts: int = Field(default=3, ge=1)
    retry_base_delay_ms: int = Field(default=1000, gt=0)
    # Messages one consumer processes at once (applied as the channel's prefetch_count).
    # The work is I/O-bound (gateway, webhook, DB), so this is cheap concurrency.
    consumer_concurrency: int = Field(default=10, gt=0)

    # Payment gateway emulation
    gateway_min_delay: float = Field(default=2.0, ge=0)
    gateway_max_delay: float = Field(default=5.0, ge=0)
    gateway_success_rate: float = Field(default=0.9, ge=0, le=1)

    webhook_timeout: float = Field(default=10.0, gt=0)

    log_level: str = "INFO"


class ApiSettings(Settings):
    # Required, no default: the API must never start with a well-known key
    api_key: str = Field(min_length=1)


@lru_cache
def get_settings() -> Settings:
    return Settings()


@lru_cache
def get_api_settings() -> ApiSettings:
    return ApiSettings()  # type: ignore[call-arg]  # api_key comes from the environment
