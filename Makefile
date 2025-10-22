.PHONY: install run docker-build docker-run clean lint format test qa demo

install:
	python -m pip install --upgrade pip
	python -m pip install -e .[dev]

run:
	python -m scripts.camels run

lint:
	python -m ruff check camels scripts tests

format:
	python -m black camels scripts tests

test:
	python -m pytest

qa: lint test

demo:
	python -m scripts.demo_seed

docker-build:
	docker build -t camels:latest .

docker-run:
	docker run --rm --env-file .env.example camels:latest run

clean:
	rm -rf __pycache__ */__pycache__ .pytest_cache .ruff_cache
