"""Seed the running API with the appendix sample payloads.

This is a developer convenience: bring up the stack with `make up`, then run
`make seed`, and watch the worker logs to see the pipeline classify and
normalize each event end-to-end.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import httpx
import orjson
import typer

cli = typer.Typer(add_completion=False, help="POST appendix sample payloads to the running API.")

FIXTURES_DIR = Path(__file__).resolve().parents[2] / "tests" / "fixtures" / "payloads"


@cli.command()
def seed(
    base_url: str = typer.Option("http://localhost:8000", "--base-url"),
    timeout: float = typer.Option(10.0, "--timeout"),
) -> None:
    """POST every payload under tests/fixtures/payloads."""

    asyncio.run(_run(base_url, timeout))


async def _run(base_url: str, timeout: float) -> None:
    if not FIXTURES_DIR.exists():
        typer.echo(f"fixtures dir not found: {FIXTURES_DIR}", err=True)
        raise typer.Exit(code=1)

    files = sorted(FIXTURES_DIR.glob("*.json"))
    if not files:
        typer.echo("no fixture files found", err=True)
        raise typer.Exit(code=1)

    async with httpx.AsyncClient(base_url=base_url, timeout=timeout) as client:
        for f in files:
            payload = orjson.loads(f.read_bytes())
            r = await client.post("/webhooks", json=payload)
            r.raise_for_status()
            typer.echo(f"{f.name:30} -> {r.json()}")


def main() -> None:
    cli()


if __name__ == "__main__":
    cli()
