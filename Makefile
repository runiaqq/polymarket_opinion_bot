PYTHON ?= python
PIP ?= pip

.PHONY: install test test-stress lint

install:
	$(PIP) install -r requirements.txt

test:
	$(PYTHON) -m pytest -q

test-stress:
	$(PYTHON) -m pytest -q -m stress

lint:
	$(PYTHON) -m pytest --collect-only







