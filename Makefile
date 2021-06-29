.DEFAULT_GOAL := all
isort = isort balsam tests
black = black --target-version py37 balsam tests

.venv:
	python3 -m venv .venv
	.venv/bin/python -m pip install --upgrade wheel pip
	
.PHONY: install-dev-venv
install-dev-venv: .venv
	.venv/bin/python -m pip install --no-cache-dir -r requirements/dev.txt
	.venv/bin/python -m pip install --no-cache-dir -e .

.PHONY: install-dev
install-dev:
	python -m pip install --upgrade wheel pip
	python -m pip install --no-cache-dir -r requirements/dev.txt
	python -m pip install --no-cache-dir -e .

.PHONY: format
format:
	$(isort)
	$(black)

.PHONY: lint
lint:
	flake8 balsam/ tests/
	$(isort) --check-only --df
	$(black) --check --diff

.PHONY: mypy
mypy:
	mypy --config-file setup.cfg --package balsam

.PHONY: generate-api
generate-api:
	python balsam/schemas/api_generator.py > balsam/_api/models.py

.PHONY: validate-defaults
validate-defaults:
	python balsam/config/defaults/validate.py
	
.PHONY: test-unit
test-unit:
	pytest tests/units --cov=balsam --cov-config setup.cfg

.PHONY: test-api
test-api:
	pytest tests/server tests/api --cov=balsam --cov-config setup.cfg

.PHONY: test-site-integ
test-site-integ:
	pytest tests/site_integration --cov=balsam --cov-config setup.cfg

.PHONY: testcov
testcov:
	pytest tests/units tests/server tests/api tests/site_integration --cov=balsam --cov-config setup.cfg --cov-report=html

.PHONY: all
all: generate-api validate-defaults format lint mypy testcov

.PHONY: build-container
build-container:
	docker-compose build

.PHONY: test-container
test-container:
	docker exec -e BALSAM_LOG_DIR="/balsam/log" -e BALSAM_TEST_API_URL="http://localhost:8000" gunicorn make testcov
