.PHONY: install run docker-build docker-run clean

install:
	python -m venv .venv && . .venv/bin/activate && pip install --upgrade pip && pip install -r requirements.txt

run:
	. .venv/bin/activate && python -m scripts.camels run

docker-build:
	docker build -t camels:latest .

docker-run:
	docker run --rm --env-file .env.example camels:latest run

clean:
	rm -rf .venv __pycache__ */__pycache__
