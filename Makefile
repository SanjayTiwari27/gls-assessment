.PHONY: help install dev up down logs migrate seed test test-unit test-e2e replay api worker fmt lint typecheck clean

PY ?= python
PIP ?= pip
COMPOSE ?= docker compose

help:
	@echo "Common targets:"
	@echo "  install       - install runtime + dev deps into current env"
	@echo "  up            - bring up the full stack (postgres, redis, api, worker)"
	@echo "  down          - tear down the full stack"
	@echo "  logs          - tail compose logs"
	@echo "  migrate       - apply database migrations"
	@echo "  seed          - POST appendix sample payloads at the running API"
	@echo "  test          - run all tests"
	@echo "  test-unit     - run unit tests (skip those needing live infra)"
	@echo "  test-e2e      - run e2e tests (require docker compose up)"
	@echo "  replay        - run the replay CLI (pass ARGS=...)"
	@echo "  api           - run the API locally (requires postgres+redis env vars)"
	@echo "  worker        - run the worker locally"
	@echo "  fmt / lint    - ruff format / lint"
	@echo "  typecheck     - mypy"

install:
	$(PIP) install -e ".[dev]"

up:
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down -v

logs:
	$(COMPOSE) logs -f --tail=200

migrate:
	$(PY) -m app.tools.migrate

seed:
	$(PY) -m app.tools.seed --base-url $${API_URL:-http://localhost:8000}

test:
	$(PY) -m pytest -q

test-unit:
	$(PY) -m pytest -q -m "not e2e"

test-e2e:
	$(PY) -m pytest -q -m e2e

replay:
	$(PY) -m app.tools.replay $(ARGS)

api:
	uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload

worker:
	$(PY) -m arq app.workers.processor.WorkerSettings

fmt:
	ruff format app tests

lint:
	ruff check app tests

typecheck:
	mypy app

clean:
	find . -name "__pycache__" -type d -prune -exec rm -rf {} +
	rm -rf .pytest_cache .ruff_cache .mypy_cache build dist *.egg-info
