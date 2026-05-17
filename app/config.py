"""Centralized application configuration loaded from env vars.

Settings are immutable and read once at process start. Anything that varies per
request belongs in a context object, not here.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Service-wide configuration."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    database_url: str = Field(
        default="postgresql://gls:gls@localhost:5432/gls",
        description="asyncpg DSN. The 'postgresql+asyncpg://' prefix is normalized away.",
    )
    db_min_pool_size: int = 2
    db_max_pool_size: int = 10

    redis_url: str = "redis://localhost:6379/0"

    log_level: str = "INFO"

    # LLM
    llm_provider: Literal["stub", "openai"] = "stub"
    openai_api_key: str = ""
    openai_model: str = "gpt-4o-mini"
    llm_request_timeout_s: float = 20.0
    llm_global_daily_budget_usd: float = 5.0
    llm_per_vendor_daily_budget_usd: float = 1.0

    # Hot path
    receiver_max_payload_bytes: int = 1_000_000  # 1 MB
    webhook_vendor_secrets: dict[str, str] = Field(
        default_factory=dict,
        description="Optional map of vendor_id -> shared secret for HMAC webhook signature verification.",
    )
    webhook_signature_header: str = "X-Signature"
    webhook_signature_enforce: bool = False

    # Worker
    worker_max_retries: int = 5
    worker_retry_base_s: float = 1.0

    # Versioning
    prompt_version: str = "v1"
    target_schema_version: str = "v1"

    @property
    def asyncpg_dsn(self) -> str:
        """asyncpg accepts the same DSN, but strip any sqlalchemy-style prefix."""
        return self.database_url.replace("postgresql+asyncpg://", "postgresql://")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
