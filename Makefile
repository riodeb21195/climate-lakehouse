.DEFAULT_GOAL := help
.PHONY: help install up down download bronze silver silver-full gold gold-full reconcile compact rollback benchmark dashboard run backfill lint format test clean

help: ## Ziele auflisten
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-12s %s\n", $$1, $$2}'

install: ## .venv anlegen und pre-commit-Hooks installieren
	uv sync
	uv run pre-commit install

.env:
	cp .env.example .env
	@echo "Created .env from .env.example - review the passwords before sharing anything."

up: .env ## Image bauen, Stack starten und warten, bis alles healthy ist
	docker compose up -d --build --wait

down: ## Stack und Dashboard stoppen (Daten-Volumes bleiben)
	docker compose --profile dashboard down

download: .env ## Neue/geänderte DWD-Dateien in die Raw-Zone laden
	uv run python -m climate_lakehouse.ingest.download

SPARK_RUN = docker compose run --rm --build jobs

bronze: .env ## Neue Raw-Dateien nach Bronze laden (Spark-Container)
	$(SPARK_RUN) climate_lakehouse.transform.bronze

silver: .env ## Silver aus neuen Bronze-Zeilen aktualisieren, mit Quality Gates (Spark-Container)
	$(SPARK_RUN) climate_lakehouse.transform.silver

silver-full: .env ## Silver komplett neu berechnen, z. B. nach Änderung von START_YEAR
	$(SPARK_RUN) climate_lakehouse.transform.silver --full-refresh

gold: .env ## Gold für Dekaden mit neuen Silver-Zeilen aktualisieren (Spark-Container)
	$(SPARK_RUN) climate_lakehouse.transform.gold

gold-full: .env ## Gold komplett neu berechnen, z. B. nach silver-full
	$(SPARK_RUN) climate_lakehouse.transform.gold --full-refresh

reconcile: .env ## Prüfen, ob jede Schicht ihre Quelle vollständig abbildet (Spark-Container)
	$(SPARK_RUN) climate_lakehouse.transform.reconcile

compact: .env ## Dateien kompaktieren, alte Snapshots verwerfen (auch letzter DAG-Task)
	$(SPARK_RUN) climate_lakehouse.transform.maintain compact

rollback: .env ## TABLE auf den Stand zum Zeitpunkt AT (UTC) zurücksetzen, z. B. make rollback TABLE=gold.state_decade AT="2026-10-29 12:00:00"
	$(SPARK_RUN) climate_lakehouse.transform.maintain rollback "$(TABLE)" "$(AT)"

benchmark: .env ## Abfragezeiten: Silver vs. unpartitionierte Kopie
	$(SPARK_RUN) climate_lakehouse.transform.benchmark

dashboard: .env ## Superset (http://localhost:8088) neben dem Stack starten
	docker compose --profile dashboard up -d --build --wait superset

run: ## Pipeline-DAG in Airflow auslösen (Fortschritt in der UI)
	docker compose exec airflow-scheduler airflow dags trigger climate_pipeline
	@echo "Progress: http://localhost:8080/dags/climate_pipeline"

backfill: ## DAG mit full_refresh auslösen: Silver und Gold aus Bronze neu berechnen
	docker compose exec airflow-scheduler airflow dags trigger climate_pipeline --conf '{"full_refresh": true}'

lint: ## Ruff-Lint, Format-Check und mypy
	uv run ruff check .
	uv run ruff format --check .
	uv run mypy

format: ## Lint-Fehler automatisch beheben und formatieren
	uv run ruff check --fix .
	uv run ruff format .

test: ## Unit- und Integrationstests mit Coverage (braucht Docker)
	uv run pytest --cov --cov-report=term-missing

clean: ## Alles stoppen, ALLE Volumes LÖSCHEN (Daten, Airflow-Historie, Dashboard), Caches entfernen
	docker compose --profile dashboard --profile jobs down -v --remove-orphans
	rm -rf .pytest_cache .mypy_cache .ruff_cache .coverage coverage.xml htmlcov dist
