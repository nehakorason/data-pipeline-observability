# Makefile — convenience targets for local development
# Usage: make <target>

.PHONY: help up down logs test lint clean

help:
	@echo ""
	@echo "  Data Pipeline Observability — Makefile Targets"
	@echo "  -----------------------------------------------"
	@echo "  make up       Start all services (build if needed)"
	@echo "  make down     Stop and remove containers"
	@echo "  make logs     Tail Airflow logs"
	@echo "  make test     Run pytest suite"
	@echo "  make clean    Remove containers, volumes, and caches"
	@echo ""

up:
	docker compose up --build -d
	@echo ""
	@echo "  ✅  Services started."
	@echo "  🌐  Airflow UI  →  http://localhost:8080  (admin / admin)"
	@echo "  🐘  PostgreSQL  →  localhost:5432"
	@echo ""

down:
	docker compose down

logs:
	docker compose logs -f airflow-scheduler airflow-webserver

test:
	@echo "Running pytest..."
	PYTHONPATH=src pytest tests/ -v

lint:
	@echo "Running ruff (if installed)..."
	ruff check src/ tests/ dags/ || echo "ruff not installed — skipping."

clean:
	docker compose down -v
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	@echo "Clean complete."
