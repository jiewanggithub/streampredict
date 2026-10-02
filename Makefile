SHELL := /bin/sh
CONDA_ENV := streampredict
CONDA_RUN := conda run -n $(CONDA_ENV)
export PRE_COMMIT_HOME := $(CURDIR)/.cache/pre-commit

.DEFAULT_GOAL := help

.PHONY: help setup format lint format-check typecheck test validate check hooks clean

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
	$(CONDA_RUN) mypy tools tests

test: ## Run the test suite
	$(CONDA_RUN) pytest

validate: ## Validate repository foundation files and whitespace
	$(CONDA_RUN) python tools/validate_project.py
	$(CONDA_RUN) python tools/check_whitespace.py

check: lint format-check typecheck test validate ## Run all local quality gates

hooks: ## Install pre-commit hooks for this repository
	$(CONDA_RUN) pre-commit install

clean: ## Remove local Python caches
	find . -type d -name __pycache__ -prune -exec rm -r {} +
	find . -type d -name .pytest_cache -prune -exec rm -r {} +
	find . -type d -name .mypy_cache -prune -exec rm -r {} +
	find . -type d -name .ruff_cache -prune -exec rm -r {} +
