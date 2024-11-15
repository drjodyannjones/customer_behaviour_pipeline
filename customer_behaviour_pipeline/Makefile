# Makefile

.PHONY: setup lint test

setup:
\tpoetry install
\tdirenv allow

lint:
\tpoetry run flake8 customer_behaviour tests

test:
\tpoetry run pytest tests
