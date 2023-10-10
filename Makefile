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
	python -m build
	ls -l dist

docs-setup:  ## setup docs pages based on README.md and HISTORY.md
	sed 's|https://raw.githubusercontent.com/drivendataorg/cloudpathlib/master/docs/docs/logo.svg|logo.svg|g' README.md \
		| sed 's|https://cloudpathlib.drivendata.org/stable/||g' \
		> docs/docs/index.md
	sed 's|https://cloudpathlib.drivendata.org/stable/|../|g' HISTORY.md \
		> docs/docs/changelog.md
	python -c \
		"import sys, re; print(re.sub(r'\]\((?!http|#)([^\)]+)\)', r'](https://github.com/drivendataorg/cloudpathlib/blob/master/\1)', sys.stdin.read()), end='')" \
		< CONTRIBUTING.md \
		> docs/docs/contributing.md

docs: clean-docs docs-setup ## build the static version of the docs
	cd docs && mkdocs build

docs-serve: clean-docs docs-setup ## serve documentation to livereload while you work
	cd docs && mkdocs serve

format:  ## run black to format codebase
	black cloudpathlib tests docs

help:
	@python -c "$$PRINT_HELP_PYSCRIPT" < $(MAKEFILE_LIST)

install: clean ## install the package to the active Python's site-packages
	python setup.py install

lint: ## check style with black, flake8, and mypy
	black --check cloudpathlib tests docs
	flake8 cloudpathlib tests docs
	mypy cloudpathlib

release: dist ## package and upload a release
	twine upload dist/*

release-test: dist
	twine upload --repository pypitest dist/*

reqs:  ## install development requirements
	pip install -U -r requirements-dev.txt

test: ## run tests with mocked cloud SDKs
	python -m pytest -vv

test-debug:  ## rerun tests that failed in last run and stop with pdb at failures
	python -m pytest -n=0 -vv --lf --pdb

test-live-cloud:  ## run tests on live cloud backends
	USE_LIVE_CLOUD=1 python -m pytest -vv

perf:  ## run performance measurement suite for s3 and save results to perf-results.csv
	python tests/performance/cli.py s3 --save-csv=perf-results.csv
