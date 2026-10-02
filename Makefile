.PHONY: up down logs test smoke

up:
	docker compose up --build -d

down:
	docker compose down

logs:
	docker compose logs -f scorer score-writer interface

test:
	pytest -q

smoke:
	python scripts/send_sample.py --csv ../test.csv --rows 10
