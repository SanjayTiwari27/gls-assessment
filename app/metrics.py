"""Prometheus counters and histograms.

These are the production-grade observability hooks the architecture document
calls for. Every code path that mutates state should bump exactly one counter
so that operators can answer "what is the system doing right now?" in real
time without log scraping.
"""

from __future__ import annotations

from prometheus_client import Counter, Histogram

INGEST_TOTAL = Counter(
    "webhook_ingest_total",
    "Total webhook deliveries received.",
    labelnames=("outcome",),  # accepted | duplicated | bad_request
)

INGEST_LATENCY = Histogram(
    "webhook_ingest_latency_seconds",
    "Latency of the webhook receiver hot path.",
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5),
)

WORKER_PROCESS_TOTAL = Counter(
    "webhook_worker_process_total",
    "Worker outcomes per event.",
    labelnames=("vendor", "classification", "outcome"),
)

WORKER_LATENCY = Histogram(
    "webhook_worker_latency_seconds",
    "Latency of end-to-end worker processing.",
    buckets=(0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0),
)

LLM_CALL_TOTAL = Counter(
    "llm_call_total",
    "LLM provider invocations.",
    labelnames=("provider", "outcome"),  # outcome: hit_cache | ok | invalid | budget_exceeded
)

LLM_TOKENS = Counter(
    "llm_tokens_total",
    "Token usage by direction.",
    labelnames=("provider", "direction"),  # direction: input | output
)

STATE_TRANSITION_TOTAL = Counter(
    "state_transition_total",
    "Outcomes of state machine apply_event calls.",
    labelnames=(
        "entity_type",
        "outcome",
    ),  # outcome: applied | already_applied | stale_skipped | transition_rejected
)
