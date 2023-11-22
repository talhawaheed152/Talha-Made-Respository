#!/bin/bash

cd "$(dirname "$0")"
pip install opendatasets
pip install pandas
pip install gdown
pip install github
pip install sqlite3
python3 pipeline.py
