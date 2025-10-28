# Australian Company Pipeline - Makefile

.PHONY: help install dev-install test lint format clean docker-build docker-up docker-down dbt-run pipeline-run setup

# Default target
help:
	@echo "Australian Company Pipeline - Available Commands:"
	@echo ""
	@echo "Setup & Installation:"
	@echo "  make setup           - Complete setup (install + database + dbt)"
	@echo "  make install         - Install Python dependencies"
	@echo "  make dev-install     - Install development dependencies"
	@echo ""
	@echo "Development:"
	@echo "  make test           - Run all tests"
	@echo "  make lint           - Run linting (flake8)"
	@echo "  make format         - Format code (black)"
	@echo "  make type-check     - Run type checking (mypy)"
	@echo "  make clean          - Clean up temporary files"
	@echo ""
	@echo "Database:"
	@echo "  make db-setup       - Initialize PostgreSQL database"
	@echo "  make db-migrate     - Run database migrations"
	@echo "  make db-reset       - Reset database (WARNING: destroys data)"
	@echo ""
	@echo "dbt:"
	@echo "  make dbt-install    - Install dbt dependencies"
	@echo "  make dbt-run        - Run dbt models"
	@echo "  make dbt-test       - Run dbt tests"
	@echo "  make dbt-docs       - Generate dbt documentation"
	@echo ""
	@echo "Pipeline:"
	@echo "  make pipeline-run   - Run full ETL pipeline"
	@echo "  make pipeline-incremental - Run incremental pipeline"
	@echo "  make pipeline-status - Check pipeline status"
	@echo ""
	@echo "Docker:"
	@echo "  make docker-build   - Build Docker images"
	@echo "  make docker-up      - Start all services"
	@echo "  make docker-down    - Stop all services"
	@echo "  make docker-logs    - View service logs"

# Setup
setup: install db-setup dbt-install
	@echo "✅ Setup complete!"

install:
	pip install -r requirements.txt

dev-install: install
	pip install -r requirements.txt[dev]
	pre-commit install

# Testing
test:
	pytest tests/ -v --cov=src --cov-report=html

test-integration:
	pytest tests/integration/ -v

test-unit:
	pytest tests/unit/ -v

# Code Quality
lint:
	flake8 src/ tests/
	@echo "✅ Linting passed"

format:
	black src/ tests/
	@echo "✅ Code formatted"

type-check:
	mypy src/
	@echo "✅ Type checking passed"

quality-check: lint type-check
	@echo "✅ All quality checks passed"

# Cleaning
clean:
	find . -type d -name __pycache__ -delete
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type f -name "*.coverage" -delete
	rm -rf build/ dist/ *.egg-info/ htmlcov/ .pytest_cache/ .mypy_cache/
	rm -rf data/downloads/ data/temp/ logs/*.log

# Database
db-setup:
	@if [ ! -f .env ]; then \
		echo "❌ .env file not found. Copy .env.example to .env and configure it."; \
		exit 1; \
	fi
	@echo "🔧 Setting up database..."
	@if command -v psql >/dev/null 2>&1; then \
		psql postgresql://$(shell grep DATABASE_USER .env | cut -d '=' -f2):$(shell grep DATABASE_PASSWORD .env | cut -d '=' -f2)@$(shell grep DATABASE_HOST .env | cut -d '=' -f2):$(shell grep DATABASE_PORT .env | cut -d '=' -f2)/$(shell grep DATABASE_NAME .env | cut -d '=' -f2) -c "SELECT 1;" || \
		createdb -h $(shell grep DATABASE_HOST .env | cut -d '=' -f2) -U $(shell grep DATABASE_USER .env | cut -d '=' -f2) $(shell grep DATABASE_NAME .env | cut -d '=' -f2); \
	else \
		echo "❌ psql not found. Install PostgreSQL client."; \
		exit 1; \
	fi

db-migrate:
	@echo "🔧 Running database migrations..."
	@for file in sql/ddl/*.sql; do \
		echo "Executing $$file..."; \
		psql $(DATABASE_URL) -f $$file; \
	done
	@echo "✅ Database migrations complete"

db-reset:
	@echo "⚠️  WARNING: This will destroy all data!"
	@read -p "Are you sure? (y/N) " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		dropdb $(shell grep DATABASE_NAME .env | cut -d '=' -f2); \
		createdb $(shell grep DATABASE_NAME .env | cut -d '=' -f2); \
		make db-migrate; \
		echo "✅ Database reset complete"; \
	else \
		echo "❌ Cancelled"; \
	fi

# dbt
dbt-install:
	cd dbt && dbt deps
	@echo "✅ dbt dependencies installed"

dbt-run:
	cd dbt && dbt run
	@echo "✅ dbt models executed"

dbt-test:
	cd dbt && dbt test
	@echo "✅ dbt tests passed"

dbt-docs:
	cd dbt && dbt docs generate && dbt docs serve

dbt-debug:
	cd dbt && dbt debug

# Pipeline Operations
pipeline-run:
	python -m src.pipeline.etl_pipeline
	@echo "✅ Full pipeline execution complete"

pipeline-incremental:
	python -m src.pipeline.etl_pipeline --incremental
	@echo "✅ Incremental pipeline execution complete"

pipeline-status:
	python -m src.pipeline.etl_pipeline --status

pipeline-recent-runs:
	python -m src.pipeline.etl_pipeline --recent-runs 10

# Extraction specific
extract-common-crawl:
	python -m src.extractors.common_crawl_extractor

extract-abr:
	python -m src.extractors.abr_extractor

# Docker Operations
docker-build:
	docker-compose -f docker/docker-compose.yml build

docker-up:
	docker-compose -f docker/docker-compose.yml up -d
	@echo "✅ All services started"
	@echo "📊 Grafana: http://localhost:3000"
	@echo "🔍 Prometheus: http://localhost:9090"
	@echo "🗄️  PostgreSQL: localhost:5432"

docker-down:
	docker-compose -f docker/docker-compose.yml down
	@echo "✅ All services stopped"

docker-logs:
	docker-compose -f docker/docker-compose.yml logs -f

docker-logs-pipeline:
	docker-compose -f docker/docker-compose.yml logs -f etl_pipeline

docker-logs-db:
	docker-compose -f docker/docker-compose.yml logs -f postgres

docker-exec-pipeline:
	docker-compose -f docker/docker-compose.yml exec etl_pipeline bash

docker-exec-db:
	docker-compose -f docker/docker-compose.yml exec postgres psql -U postgres -d australian_companies

# Monitoring
monitor-db:
	@echo "📊 Database Statistics:"
	@psql $(DATABASE_URL) -c "SELECT schemaname, tablename, n_live_tup as row_count FROM pg_stat_user_tables ORDER BY n_live_tup DESC;"

monitor-pipeline:
	@echo "📈 Pipeline Statistics:"
	@python -c "from src.utils.database import DatabaseManager; import asyncio; db = DatabaseManager('$(DATABASE_URL)'); print(asyncio.run(db.get_table_stats()))"

# Data Quality
quality-report:
	@echo "📋 Data Quality Report:"
	cd dbt && dbt run -m company_summary_stats
	@psql $(DATABASE_URL) -c "SELECT * FROM analytics.company_summary_stats ORDER BY calculation_date DESC LIMIT 1;"

# Development Helpers
dev-setup: dev-install db-setup dbt-install
	@echo "🚀 Development environment ready!"

dev-test-cycle: format lint type-check test
	@echo "✅ Development cycle complete"

# Environment validation
validate-env:
	@echo "🔍 Validating environment..."
	@python -c "from src.utils.config import Config; config = Config(); print('✅ Configuration loaded successfully')"
	@python -c "from src.utils.database import test_database_connection; import asyncio; asyncio.run(test_database_connection('$(DATABASE_URL)')) and print('✅ Database connection OK')"

# Backup
backup-db:
	@echo "💾 Creating database backup..."
	pg_dump $(DATABASE_URL) > backup_$(shell date +%Y%m%d_%H%M%S).sql
	@echo "✅ Backup created"

# Production helpers
prod-deploy:
	@echo "🚀 Deploying to production..."
	@echo "⚠️  Make sure you've tested everything!"
	docker-compose -f docker/docker-compose.yml -f docker/docker-compose.prod.yml up -d --build

# Documentation
docs-build:
	@echo "📚 Building documentation..."
	# Add documentation build commands here

docs-serve:
	@echo "📚 Serving documentation..."
	# Add documentation serve commands here