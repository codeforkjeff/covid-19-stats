#!/bin/bash

source /usr/share/virtualenvwrapper/virtualenvwrapper.sh

mkvirtualenv covid-19-stats-load

python3 -m pip install --upgrade setuptools
python3 -m pip install --upgrade pip
pip install -r requirements-load.txt || exit 1

mkvirtualenv covid-19-stats-dbt

python3 -m pip install --upgrade setuptools
python3 -m pip install --upgrade pip
pip install -r requirements-dbt.txt || exit 1

python3 -m pip cache purge
