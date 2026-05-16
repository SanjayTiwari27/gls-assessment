"""LLM orchestration: cache → budget guard → call → schema validate → audit.

The orchestrator is the only place in the codebase that talks to an
``LLMProvider``. Adapters delegate here when they cannot extract everything
themselves; the universal adapter delegates here for every payload.

Hard rules baked in:

  - Cache before any network call. Cache after a successful call.
  - Refuse to call when the per-vendor or global budget is exhausted; mark the
    event ``pending_llm`` instead of dropping it.
  - Validate every output against the JSON schema. On failure, retry exactly
    once with the validation error appended to the prompt. Otherwise, the
    caller is told to mark the event ``requires_human_review``.
  - Every call (including cache hits) writes one audit row keyed by event_id.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import asyncpg
import jsonschema
import orjson

from app.config import get_settings
from app.hashing import canonical_json, llm_cache_key
from app.llm.provider import LLMProvider
from app.logging import get_logger
from app.metrics import LLM_CALL_TOTAL, LLM_TOKENS

_PROMPT_PATH = Path(__file__).resolve().parent / "prompts" / "v1_classify_extract.txt"
_SCHEMA_PATH = Path(__file__).resolve().parent / "schemas" / "v1_target_schema.json"

log = get_logger("llm.fallback")


class LLMValidationError(Exception):
    """Raised when LLM output cannot be coerced into the target schema even after retry."""


class BudgetExceeded(Exception):
    """Raised when the LLM call would exceed today's budget for this vendor or globally."""


@dataclass(slots=True)
class LLMOutcome:
    data: dict[str, Any]
    source: str             # 'llm' | 'llm_cache'
    model: str
    prompt_version: str
    schema_version: str
    tokens_in: int
    tokens_out: int
    latency_ms: int
    cost_estimate: float


def load_prompt_template() -> str:
    return _PROMPT_PATH.read_text(encoding="utf-8")


def load_target_schema() -> dict[str, Any]:
    return json.loads(_SCHEMA_PATH.read_text(encoding="utf-8"))


class BudgetGuard:
    """Postgres-backed daily budget guard.

    A real production system would use a token-bucket in Redis; here we keep
    one row per (vendor_id, day) and short-circuit when the cap is hit. This
    is good enough to demonstrate the contract and is exact across processes.
    """

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def allow(self, *, vendor_id: str, estimated_cost: float) -> bool:
        settings = get_settings()
        async with self._pool.acquire() as conn:
            today = datetime.now(UTC).date()
            row = await conn.fetchrow(
                """
                SELECT
                    COALESCE(SUM(cost_estimate) FILTER (WHERE created_at::date = $1), 0) AS today_total,
                    COALESCE(SUM(cost_estimate) FILTER (WHERE created_at::date = $1 AND substr(decision, 1, 100) = $2), 0) AS today_vendor
                FROM llm_audit
                """,
                today,
                vendor_id,
            )
            today_total = float(row["today_total"] or 0)
            today_vendor = float(row["today_vendor"] or 0)
            if today_total + estimated_cost > settings.llm_global_daily_budget_usd:
                return False
            if today_vendor + estimated_cost > settings.llm_per_vendor_daily_budget_usd:
                return False
        return True


class LLMFallback:
    """High-level orchestration."""

    def __init__(self, *, provider: LLMProvider, pool: asyncpg.Pool, budget: BudgetGuard | None = None) -> None:
        self._provider = provider
        self._pool = pool
        self._budget = budget or BudgetGuard(pool)
        self._prompt_template = load_prompt_template()
        self._schema = load_target_schema()
        settings = get_settings()
        self._prompt_version = settings.prompt_version
        self._schema_version = settings.target_schema_version

    @property
    def schema(self) -> dict[str, Any]:
        return self._schema

    async def classify_extract(
        self,
        *,
        event_id: str,
        vendor_hint: str,
        payload: dict[str, Any],
    ) -> LLMOutcome:
        cache_key = llm_cache_key(self._prompt_version, payload, self._schema_version)

        cached = await self._cache_get(cache_key)
        if cached is not None:
            LLM_CALL_TOTAL.labels(provider=self._provider.name, outcome="hit_cache").inc()
            outcome = LLMOutcome(
                data=cached["output"],
                source="llm_cache",
                model=cached.get("model") or self._provider.model,
                prompt_version=cached.get("prompt_version") or self._prompt_version,
                schema_version=cached.get("schema_version") or self._schema_version,
                tokens_in=int(cached.get("tokens_in") or 0),
                tokens_out=int(cached.get("tokens_out") or 0),
                latency_ms=int(cached.get("latency_ms") or 0),
                cost_estimate=float(cached.get("cost_estimate") or 0.0),
            )
            await self._audit(event_id=event_id, outcome=outcome, decision=vendor_hint)
            return outcome

        # Budget guard before any network call.
        if not await self._budget.allow(vendor_id=vendor_hint, estimated_cost=0.001):
            LLM_CALL_TOTAL.labels(provider=self._provider.name, outcome="budget_exceeded").inc()
            raise BudgetExceeded(vendor_hint)

        prompt = self._render_prompt(payload)

        started = time.perf_counter()
        try:
            llm_result = await self._provider.complete(
                prompt=prompt,
                schema=self._schema,
                temperature=0.0,
            )
            data = self._parse_and_validate(llm_result.text)
        except (json.JSONDecodeError, jsonschema.ValidationError) as first_err:
            log.warning("llm_first_pass_invalid", event_id=event_id, error=type(first_err).__name__)
            retry_prompt = (
                prompt
                + "\n\nThe previous output was rejected by the schema with this error: "
                + str(first_err)
                + "\nReturn a corrected JSON object that strictly satisfies the schema."
            )
            try:
                llm_result = await self._provider.complete(
                    prompt=retry_prompt,
                    schema=self._schema,
                    temperature=0.0,
                )
                data = self._parse_and_validate(llm_result.text)
            except (json.JSONDecodeError, jsonschema.ValidationError) as second_err:
                LLM_CALL_TOTAL.labels(provider=self._provider.name, outcome="invalid").inc()
                raise LLMValidationError(str(second_err)) from second_err

        latency_ms = int((time.perf_counter() - started) * 1000)
        LLM_CALL_TOTAL.labels(provider=self._provider.name, outcome="ok").inc()
        LLM_TOKENS.labels(provider=self._provider.name, direction="input").inc(llm_result.tokens_in)
        LLM_TOKENS.labels(provider=self._provider.name, direction="output").inc(llm_result.tokens_out)

        outcome = LLMOutcome(
            data=data,
            source="llm",
            model=llm_result.model,
            prompt_version=self._prompt_version,
            schema_version=self._schema_version,
            tokens_in=llm_result.tokens_in,
            tokens_out=llm_result.tokens_out,
            latency_ms=latency_ms,
            cost_estimate=llm_result.cost_estimate,
        )

        await self._cache_set(cache_key=cache_key, outcome=outcome)
        await self._audit(event_id=event_id, outcome=outcome, decision=vendor_hint)
        return outcome

    # --------------------------------------------------------------------- #
    # internals
    # --------------------------------------------------------------------- #

    def _render_prompt(self, payload: dict[str, Any]) -> str:
        return self._prompt_template.replace(
            "{{PAYLOAD}}",
            canonical_json(payload).decode("utf-8"),
        )

    def _parse_and_validate(self, text: str) -> dict[str, Any]:
        # Strip any accidental code fences.
        cleaned = text.strip()
        if cleaned.startswith("```"):
            cleaned = cleaned.split("```", 2)[-1]
            cleaned = cleaned.replace("json\n", "", 1).strip()
        data = orjson.loads(cleaned)
        jsonschema.validate(instance=data, schema=self._schema)
        if not isinstance(data, dict):
            raise jsonschema.ValidationError("top-level must be an object")
        return data

    async def _cache_get(self, cache_key: str) -> dict[str, Any] | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT output, model, prompt_version, schema_version,
                       tokens_in, tokens_out, latency_ms, cost_estimate
                FROM llm_cache
                WHERE cache_key = $1
                """,
                cache_key,
            )
            if row is None:
                return None
            return dict(row)

    async def _cache_set(self, *, cache_key: str, outcome: LLMOutcome) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO llm_cache (cache_key, output, model, prompt_version, schema_version,
                                       tokens_in, tokens_out, latency_ms, cost_estimate)
                VALUES ($1, $2::jsonb, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (cache_key) DO NOTHING
                """,
                cache_key,
                outcome.data,
                outcome.model,
                outcome.prompt_version,
                outcome.schema_version,
                outcome.tokens_in,
                outcome.tokens_out,
                outcome.latency_ms,
                outcome.cost_estimate,
            )

    async def _audit(self, *, event_id: str, outcome: LLMOutcome, decision: str) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO llm_audit (event_id, source, model, prompt_version, schema_version,
                                       tokens_in, tokens_out, latency_ms, cost_estimate, decision)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                """,
                event_id,
                outcome.source,
                outcome.model,
                outcome.prompt_version,
                outcome.schema_version,
                outcome.tokens_in,
                outcome.tokens_out,
                outcome.latency_ms,
                outcome.cost_estimate,
                decision,
            )
