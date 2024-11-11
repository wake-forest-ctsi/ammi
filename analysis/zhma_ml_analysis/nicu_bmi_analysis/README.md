# ML Analysis

# Getting started
Create a new environment: `python -m venv .venv`

activate the .venv: `cd .venv/Scripts && activate && cd ../../` or `source .venv/bin/activate`

Install dependencies: `python -m pip install -r requirements.txt`

### Files
get_nicu_bmi.sql: the sql file that queries the pcornet tables to get the get_nicu_admission_05_16.csv file. This csv file can be reproduced by running the sql query. The output includes whether or not babies were admitted to NICU. It also computes the BMI for the first three months of pregnancy.

nicu_bmi.ipynb: the jupyter notebook to analyze the csv file.