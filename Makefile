.DEFAULT_GOAL := help
.PHONY: help install lint format test clean

help: ## List targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-10s %s\n", $$1, $$2}'

install: ## Create .venv and install pre-commit hooks
	uv sync
	uv run pre-commit install

lint: ## Ruff lint + format check + mypy
	uv run ruff check .
	uv run ruff format --check .
	uv run mypy

format: ## Auto-fix lint issues and format
	uv run ruff check --fix .
	uv run ruff format .

test: ## Unit tests with coverage
	uv run pytest -m "not integration" --cov --cov-report=term-missing

clean: ## Remove caches and local build output
	rm -rf .pytest_cache .mypy_cache .ruff_cache .coverage coverage.xml htmlcov dist
