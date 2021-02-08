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

balsam/api/models.py: balsam/api/bases.py  balsam/server/routers/filters.py
	python balsam/schemas/api_generator.py > balsam/api/models.py

.PHONY: validate-defaults
validate-defaults:
	python balsam/config/defaults/validate.py
	

.PHONY: test-api
test-api:
	pytest tests/server --cov=balsam --cov-config setup.cfg
	pytest tests/api --cov=balsam --cov-append --cov-config setup.cfg

.PHONY: testcov
testcov: test-api
	@echo "building coverage html"
	@coverage html

.PHONY: all
all: balsam/api/models.py validate-defaults format lint mypy testcov
