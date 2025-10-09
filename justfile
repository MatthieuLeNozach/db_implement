# ------------------------
# ENVIRONMENT SETUP
# ------------------------
init:
    uv python install 3.13
    uv tool install ruff@latest
    uv tool install pre-commit
    pre-commit install


# ------------------------
# APPLICATION RUNNERS
# ------------------------
# Run Flask app only
run:
    uv sync
    uv run python src/app.py

# Run sqlite-web only
run-db:
    uv run python -m sqlite_web database/trade_data.db --host 0.0.0.0 --port 8081

# Run both Flask app and sqlite-web
run-all:
    #!/usr/bin/env bash
    uv sync
    uv run python -m sqlite_web database/trade_data.db --host 0.0.0.0 --port 8081 &
    SQLITE_PID=$!
    echo "sqlite-web running on http://localhost:8081 (PID: $SQLITE_PID)"
    trap "kill $SQLITE_PID 2>/dev/null" EXIT
    uv run python src/app.py

# Stop all background processes
stop:
    pkill -f sqlite_web


# ------------------------
# DATABASE MANAGEMENT
# ------------------------

# Initialize and populate the database with all default data.
db-populate:
    uv sync
    uv run python scripts/populate_db.py

# Populate only a subset of tables.
# Usage: just db-populate-tables products customers
db-populate-tables *TABLES:
    uv sync
    uv run python scripts/populate_db.py --tables {{TABLES}}

# Import only format configurations, dropping and recreating the table first.
db-populate-formats:
    uv sync
    uv run python scripts/populate_db.py --tables formats --drop-formats


# ------------------------
# ALEMBIC MIGRATIONS
# ------------------------

# # Initialize Alembic environment. Only run once.
# alembic-init:
#     uv sync
#     uv run alembic init -t generic alembic

# # Generate a new revision script automatically detecting model changes.
# # Usage: just alembic-revision -m "Add new column to product table"
# alembic-revision -m NAME:
#     uv sync
#     uv run alembic revision --autogenerate -m "{{NAME}}"

# # Apply all outstanding migrations to the database.
# alembic-upgrade:
#     uv sync
#     uv run alembic upgrade head

# # Revert the last migration applied to the database.
# alembic-downgrade:
#     uv sync
#     uv run alembic downgrade -1

# ------------------------
# CI/CD & DOCKER
# ------------------------

build-production:
    docker build -f .production/Dockerfile --target production -t extract-po .

run-production: build-production
    docker run --rm -e DEFAULT_USER_PWD=test -p 8000:8000 extract-po

docker_gcs_path := "europe-west1-docker.pkg.dev/medelys-349301/medelys/extract-po"

build-production-ci TAG:
    docker build -f .production/Dockerfile --target production -t {{docker_gcs_path}}:{{TAG}} -t {{docker_gcs_path}}:latest .

push-production-ci TAG: (build-production-ci TAG)
    docker push {{docker_gcs_path}}:{{TAG}}

deploy-production-ci TAG:
    gcloud run deploy extract-po --image {{docker_gcs_path}}:{{TAG}} --region europe-west1 --platform managed --allow-unauthenticated