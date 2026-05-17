"""LLMProvider protocol and selection.

The orchestrator (`LLMFallback`) treats every provider identically. Swapping
providers is a one-line change in config.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(slots=True)
class LLMResult:
    text: str
    model: str
    tokens_in: int
    tokens_out: int
    latency_ms: int
    cost_estimate: float


class LLMProvider(Protocol):
    name: str
    model: str

    async def complete(
        self,
        *,
        prompt: str,
        schema: dict[str, Any],
        temperature: float = 0.0,
    ) -> LLMResult: ...


def build_default_provider() -> LLMProvider:
    """Build the configured LLM provider (OpenAI)."""

    from app.config import get_settings

    settings = get_settings()

    from app.llm.openai_provider import OpenAILLM

    return OpenAILLM(
        api_key=settings.openai_api_key,
        model=settings.openai_model,
        timeout_s=settings.llm_request_timeout_s,
    )
