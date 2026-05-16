"""Architectural guard: the receiver must not import LLM or state-machine code.

If this test fails, someone has put business logic on the hot path. Move it to
the worker. The whole point of the receiver is to do O(1) work and return 202.
"""

from __future__ import annotations

from pathlib import Path

import pytest

RECEIVER_PATH = Path(__file__).resolve().parents[1] / "app" / "api" / "receiver.py"


@pytest.mark.parametrize(
    "forbidden_import",
    [
        "from app.llm",
        "import app.llm",
        "from app.domain.state_machine",
        "import app.domain.state_machine",
        "from app.adapters.registry",   # adapter resolution is a worker concern
        "from app.workers.pipeline",
    ],
)
def test_receiver_does_not_import_business_modules(forbidden_import: str) -> None:
    src = RECEIVER_PATH.read_text(encoding="utf-8")
    assert forbidden_import not in src, (
        f"Receiver hot path imports forbidden module: {forbidden_import!r}. "
        "Move that work to the worker."
    )
