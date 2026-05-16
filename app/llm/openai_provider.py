"""OpenAI provider — Structured Outputs.

Selected when ``LLM_PROVIDER=openai``. Uses the chat-completions API with
``response_format={"type": "json_schema"}`` so the provider itself enforces
schema conformance. We still re-validate the returned JSON in the orchestrator
because providers occasionally drift.
"""

from __future__ import annotations

import time
from typing import Any

import httpx

from app.llm.provider import LLMResult

# Per-1k-token pricing for cost estimation. These are rough; real cost
# attribution should pull from the provider's billing API. Update on model
# bumps.
_PRICING: dict[str, tuple[float, float]] = {
    "gpt-4o-mini": (0.000150, 0.000600),
    "gpt-4o": (0.005000, 0.015000),
    "gpt-4.1-mini": (0.000400, 0.001600),
}


class OpenAILLM:
    name = "openai"

    def __init__(
        self,
        *,
        api_key: str,
        model: str,
        timeout_s: float = 60.0,
        base_url: str = "https://api.openai.com/v1",
    ) -> None:
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY is required when LLM_PROVIDER=openai")
        self.model = model
        self._client = httpx.AsyncClient(
            base_url=base_url,
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=timeout_s,
        )

    async def complete(
        self,
        *,
        prompt: str,
        schema: dict[str, Any],
        temperature: float = 0.0,
    ) -> LLMResult:
        body = {
            "model": self.model,
            "temperature": temperature,
            "messages": [
                {
                    "role": "system",
                    "content": "Return only a JSON object that conforms to the provided schema. No prose.",
                },
                {"role": "user", "content": prompt},
            ],
            "response_format": {
                "type": "json_schema",
                "json_schema": {
                    "name": "WebhookExtractionV1",
                    "schema": schema,
                    "strict": True,
                },
            },
        }
        started = time.perf_counter()
        resp = await self._client.post("/chat/completions", json=body)
        resp.raise_for_status()
        data = resp.json()
        latency_ms = int((time.perf_counter() - started) * 1000)

        try:
            text = data["choices"][0]["message"]["content"]
        except (KeyError, IndexError) as exc:
            raise RuntimeError(f"unexpected openai response shape: {data}") from exc

        usage = data.get("usage", {}) or {}
        tokens_in = int(usage.get("prompt_tokens") or 0)
        tokens_out = int(usage.get("completion_tokens") or 0)

        rate_in, rate_out = _PRICING.get(self.model, (0.0, 0.0))
        cost = (tokens_in / 1000.0) * rate_in + (tokens_out / 1000.0) * rate_out

        return LLMResult(
            text=text,
            model=self.model,
            tokens_in=tokens_in,
            tokens_out=tokens_out,
            latency_ms=latency_ms,
            cost_estimate=round(cost, 6),
        )

    async def aclose(self) -> None:
        await self._client.aclose()
