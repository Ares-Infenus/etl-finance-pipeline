.PHONY: venv install format lint test precommit

venv:
	python -m venv .venv

install:
	. .venv/bin/activate && pip install -r requirements.txt

format:
	. .venv/bin/activate && black .

lint:
	. .venv/bin/activate && ruff check .

test:
	. .venv/bin/activate && pytest -q

precommit:
	pre-commit run --all-files
