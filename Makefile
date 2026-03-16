.PHONY: sync lint typecheck test check help

sync:
	uv sync

lint:
	uv run ruff check .

typecheck:
	uv run mypy src

test:
	uv run pytest

check: lint typecheck test

help:
	uv run pmat --help

