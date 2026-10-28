.DEFAULT_GOAL := help
.PHONY: help install up down download lint format test clean

help: ## List targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-10s %s\n", $$1, $$2}'

install: ## Create .venv and install pre-commit hooks
	uv sync
	uv run pre-commit install

.env:
	cp .env.example .env
	@echo "Created .env from .env.example - review the passwords before sharing anything."

up: .env ## Start the stack and wait until it is healthy
	docker compose up -d --wait

down: ## Stop the stack (data volumes are kept)
	docker compose down

download: .env ## Fetch new/changed DWD files into the raw zone
	uv run python -m climate_lakehouse.ingest.download

lint: ## Ruff lint + format check + mypy
	uv run ruff check .
	uv run ruff format --check .
	uv run mypy

format: ## Auto-fix lint issues and format
	uv run ruff check --fix .
	uv run ruff format .

test: ## Unit and integration tests (needs Docker) with coverage
	uv run pytest --cov --cov-report=term-missing

clean: ## Stop the stack, DELETE its volumes and remove local caches
	docker compose down -v
	rm -rf .pytest_cache .mypy_cache .ruff_cache .coverage coverage.xml htmlcov dist
