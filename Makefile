.DEFAULT_GOAL := all
isort = isort balsam tests
black = black --target-version py37 balsam tests

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
test-api: test-unit
	pytest tests/server tests/api --cov=balsam --cov-append --cov-config setup.cfg

.PHONY: test-site-integ
test-site-integ: test-api
	pytest tests/site_integration --cov=balsam --cov-append --cov-config setup.cfg

.PHONY: testcov
testcov: test-site-integ
	@echo "building coverage html"
	@coverage html

.PHONY: all
all: generate-api validate-defaults format lint mypy testcov
