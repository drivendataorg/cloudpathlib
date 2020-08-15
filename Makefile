.PHONY: clean clean-docs clean-pyc clean-test clean-build docs format install lint release release-test test help
.DEFAULT_GOAL := help

define PRINT_HELP_PYSCRIPT
import re, sys

for line in sys.stdin:
	match = re.match(r'^([a-zA-Z_-]+):.*?## (.*)$$', line)
	if match:
		target, help = match.groups()
		print("%-20s %s" % (target, help))
endef
export PRINT_HELP_PYSCRIPT

clean: clean-build clean-pyc clean-test ## remove all build, test, coverage and Python artifacts

clean-build: ## remove build artifacts
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -f {} +

clean-pyc: ## remove Python file artifacts
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

clean-test: ## remove test and coverage artifacts
	rm -fr .tox/
	rm -f .coverage
	rm -fr htmlcov/
	rm -fr .pytest_cache

dist: clean ## builds source and wheel package
	python setup.py sdist
	python setup.py bdist_wheel
	ls -l dist

docs: clean-docs
	for cmd in clean configure export install ; do \
		bash docs/_scripts/generate_command_reference.sh $$cmd; \
	done
	cd docs && mkdocs build

format:
	black nbautoexport tests

help:
	@python -c "$$PRINT_HELP_PYSCRIPT" < $(MAKEFILE_LIST)

install: clean ## install the package to the active Python's site-packages
	python setup.py install

lint: ## check style with flake8
	black --check nbautoexport tests
	flake8 nbautoexport tests
	mypy nbautoexport tests

release: dist ## package and upload a release
	twine upload dist/*

release-test: dist
	twine upload --repository pypitest dist/*

reqs:
	pip install -r requirements-dev.txt

test: ## run tests quickly with the default Python
	python -m pytest -vv