#!/bin/bash

cd "$(dirname "$0")"
pip install opendatasets
pip install pandas
pip install gdown
pip install github
pip install sqlite3
pip install pandas pyarrow requests
python3 pipeline.py
