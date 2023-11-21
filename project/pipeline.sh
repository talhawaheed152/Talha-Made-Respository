#!/bin/bash

# Change to the directory where the script is located
cd "$(dirname "$0")"

# Run the Python script
pip install opendatasets
pip install pandas
pip install gdown
pip install github
pip install sqlite3
python3 pipeline.py
