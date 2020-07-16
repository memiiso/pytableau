#!/usr/bin/env bash

# build and test
python3 --version
python3 -m pip install coverage pylint pytest
python3 -m pylint pytableau setup.py
python3 -m compileall -f pytableau setup.py
python3 -m coverage report -m ./pytableau/*.py setup.py
python3 setup.py -q install --user --force
