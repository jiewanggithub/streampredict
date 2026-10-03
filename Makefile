SHELL := /bin/sh
CONDA_ENV := streampredict
CONDA_RUN := conda run -n $(CONDA_ENV)
# Call the env's interpreter by absolute path: an activated Conda env or a system Python earlier on
# PATH can otherwise shadow `python` even inside `conda run`.
ENV_PREFIX := $(shell conda run -n $(CONDA_ENV) sh -c 'echo $$CONDA_PREFIX' 2>/dev/null)
PYTHON := $(CONDA_RUN) $(ENV_PREFIX)/bin/python
COMPOSE := docker compose -f infra/docker/compose.yaml
export PRE_COMMIT_HOME := $(CURDIR)/.cache/pre-commit

.DEFAULT_GOAL := help

.PHONY: help setup format lint format-check typecheck test validate api-dev api-lock dashboard-install dashboard-dev dashboard-check up down logs check hooks clean

help: ## Show available commands
	@awk 'BEGIN {FS = ":.*## "; printf "StreamPredict development commands\n\n"} /^[a-zA-Z_-]+:.*## / {printf "  %-16s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

setup: ## Create or update the Conda environment
	conda env update -n $(CONDA_ENV) -f environment.yml --prune

format: ## Format Python code
	$(CONDA_RUN) ruff check --fix .
	$(CONDA_RUN) ruff format .

lint: ## Run Python lint checks
	$(CONDA_RUN) ruff check .

format-check: ## Verify Python formatting without changing files
	$(CONDA_RUN) ruff format --check .

typecheck: ## Run strict Python type checking
	$(CONDA_RUN) mypy tools tests services/api/src

test: ## Run the test suite
	$(CONDA_RUN) pytest

validate: ## Validate repository foundation files and whitespace
	$(PYTHON) tools/validate_project.py
	$(PYTHON) tools/check_whitespace.py

api-dev: ## Start the API gateway with auto-reload (needs Redis: make up or docker run redis)
	cd services/api && PYTHONPATH=src $(CONDA_RUN) --no-capture-output $(ENV_PREFIX)/bin/python -m uvicorn --factory streampredict_api.main:app_factory --reload --port 8000

api-lock: ## Regenerate services/api/requirements.txt from requirements.in
	rm -rf .cache/api-lock && $(PYTHON) -m venv .cache/api-lock
	.cache/api-lock/bin/pip install -q -r services/api/requirements.in
	{ echo "# Locked runtime dependencies generated from requirements.in by 'make api-lock'."; .cache/api-lock/bin/pip freeze; } > services/api/requirements.txt

dashboard-install: ## Install dashboard dependencies
	npm --prefix apps/dashboard install

dashboard-dev: ## Start the Next.js dashboard locally
	npm --prefix apps/dashboard run dev

dashboard-check: ## Lint, type-check, and build the dashboard
	npm --prefix apps/dashboard run check

up: ## Build and start the local stack (Redis, API, dashboard) with Docker Compose
	$(COMPOSE) up --build -d

down: ## Stop the local Docker Compose stack
	$(COMPOSE) down

logs: ## Follow logs from the local Docker Compose stack
	$(COMPOSE) logs -f

check: lint format-check typecheck test validate dashboard-check ## Run all local quality gates

hooks: ## Install pre-commit hooks for this repository
	$(CONDA_RUN) pre-commit install

clean: ## Remove local Python caches
	find . -type d -name __pycache__ -prune -exec rm -r {} +
	find . -type d -name .pytest_cache -prune -exec rm -r {} +
	find . -type d -name .mypy_cache -prune -exec rm -r {} +
	find . -type d -name .ruff_cache -prune -exec rm -r {} +
