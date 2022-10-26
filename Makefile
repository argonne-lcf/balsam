.DEFAULT_GOAL := all
isort = isort balsam tests
black = black --target-version py37 balsam tests

.PHONY: install-dev
install-dev:
	python -m pip install --upgrade wheel pip
	python -m pip install -r requirements/dev.txt

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

.PHONY: distribute
distribute:
	cp docs/README.md README.md
	python -m build
	python -m twine check dist/*
	python -m twine upload dist/*


.PHONY: all
all: generate-api validate-defaults format lint mypy testcov
