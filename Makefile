.DEFAULT_GOAL := help
.PHONY: help install up down download bronze silver silver-full gold gold-full reconcile run backfill lint format test clean

help: ## List targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-12s %s\n", $$1, $$2}'

install: ## Create .venv and install pre-commit hooks
	uv sync
	uv run pre-commit install

.env:
	cp .env.example .env
	@echo "Created .env from .env.example - review the passwords before sharing anything."

up: .env ## Build the image, start the stack and wait until it is healthy
	docker compose up -d --build --wait

down: ## Stop the stack (data volumes are kept)
	docker compose down

download: .env ## Fetch new/changed DWD files into the raw zone
	uv run python -m climate_lakehouse.ingest.download

SPARK_RUN = docker compose run --rm --build jobs

bronze: .env ## Load new raw files into the bronze tables (Spark container)
	$(SPARK_RUN) climate_lakehouse.transform.bronze

silver: .env ## Update silver from new bronze rows, with quality gates (Spark container)
	$(SPARK_RUN) climate_lakehouse.transform.silver

silver-full: .env ## Recompute silver completely, e.g. after changing START_YEAR
	$(SPARK_RUN) climate_lakehouse.transform.silver --full-refresh

gold: .env ## Update gold indicators for decades with new silver rows (Spark container)
	$(SPARK_RUN) climate_lakehouse.transform.gold

gold-full: .env ## Recompute gold completely, e.g. after silver-full
	$(SPARK_RUN) climate_lakehouse.transform.gold --full-refresh

reconcile: .env ## Check that every layer accounts for its source (Spark container)
	$(SPARK_RUN) climate_lakehouse.transform.reconcile

run: ## Trigger the pipeline DAG in Airflow (progress in the UI)
	docker compose exec airflow-scheduler airflow dags trigger climate_pipeline
	@echo "Progress: http://localhost:8080/dags/climate_pipeline"

backfill: ## Trigger the DAG with full_refresh: recompute silver and gold from bronze
	docker compose exec airflow-scheduler airflow dags trigger climate_pipeline --conf '{"full_refresh": true}'

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
