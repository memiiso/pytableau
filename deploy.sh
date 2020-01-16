#!/usr/bin/env bash

# build and test
python3 --version
pylint pytableau setup.py
python3 -m compileall -f pytableau setup.py
coverage report -m ./pytableau/*.py
coverage run --source=./tests/ -m unittest discover -s tests/
python3 setup.py -q install --user