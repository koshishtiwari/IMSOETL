.PHONY: help install install-dev test lint format clean build docs run-interactive

# Default target
help:
	@echo "Available targets:"
	@echo "  install      - Install the package"
	@echo "  install-dev  - Install development dependencies"
	@echo "  test         - Run tests"
	@echo "  lint         - Run linting checks"
	@echo "  format       - Format code"
	@echo "  clean        - Clean build artifacts"
	@echo "  build        - Build the package"
	@echo "  docs         - Generate documentation"
	@echo "  run-interactive - Run IMSOETL in interactive mode"

# Installation
install:
	pip install -e .

install-dev:
	pip install -e .[dev,connectors]
	pre-commit install

# Testing
test:
	pytest tests/ -v --cov=imsoetl --cov-report=html --cov-report=term-missing

test-watch:
	pytest-watch tests/ -- -v

# Code quality
lint:
	flake8 src/imsoetl tests/
	mypy src/imsoetl
	black --check src/imsoetl tests/
	isort --check-only src/imsoetl tests/

format:
	black src/imsoetl tests/
	isort src/imsoetl tests/

# Build and distribution
clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf htmlcov/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

build: clean
	python -m build

# Documentation
docs:
	@echo "Documentation generation not yet implemented"

# Development
run-interactive:
	python -m imsoetl interactive

run-cli:
	python -m imsoetl --help

# Docker (future)
docker-build:
	@echo "Docker support not yet implemented"

docker-run:
	@echo "Docker support not yet implemented"
