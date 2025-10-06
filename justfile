init:
    uv python install 3.13
    uv tool install ruff@latest
    uv tool install pre-commit
    pre-commit install


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