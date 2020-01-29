#!/usr/bin/env bash

# build and test
python3 --version
python3 -m pylint pytableau setup.py
python3 -m compileall -f pytableau setup.py
python3 -m coverage report -m ./pytableau/*.py
python3 -m coverage run --source=./tests/ -m unittest discover -s tests/
python3 setup.py -q install --user --force