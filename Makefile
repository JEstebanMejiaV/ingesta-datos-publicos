.PHONY: install pwt banrep lint test freeze

VENV=.venv
PYTHON=$(VENV)/bin/python
PIP=$(VENV)/bin/pip

install:
	python -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	$(PIP) install ruff pytest

pwt:
	$(PYTHON) src/pwt/pwt_loader_clean.py

banrep:
	$(PYTHON) src/banrep/banrep_consolidate_v3.py --out-dir banrep_output --flows ALL --log INFO

lint:
	$(VENV)/bin/ruff check src

test:
	$(VENV)/bin/pytest

freeze:
	$(PIP) freeze > requirements.lock.txt
