"""OpenAI provider — JSON-object mode + orchestrator-side schema validation.

Selected when ``LLM_PROVIDER=openai``. Uses the chat-completions API with
``response_format={"type": "json_object"}`` and embeds the target JSON schema
into the system message so the model knows the exact shape to produce.

We deliberately do NOT use ``response_format={"type": "json_schema", ...,
"strict": true}`` because OpenAI's strict subset forbids ``additionalProperties:
true``, and our schema needs freeform maps (``reference_ids``,
``linked_references``) for vendor-id round-tripping. The orchestrator
(``app.llm.fallback.LLMFallback``) re-validates every output with ``jsonschema``
and runs one self-correcting retry on validation failure, which is the actual
safety net regardless of provider mode.
"""

from __future__ import annotations

import json
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
        system_msg = (
            "Return only a single JSON object. No prose, no markdown fences, no commentary. "
            "The object MUST conform exactly to this JSON schema:\n"
            + json.dumps(schema, separators=(",", ":"))
        )
        body = {
            "model": self.model,
            "temperature": temperature,
            "messages": [
                {"role": "system", "content": system_msg},
                {"role": "user", "content": prompt},
            ],
            "response_format": {"type": "json_object"},
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
