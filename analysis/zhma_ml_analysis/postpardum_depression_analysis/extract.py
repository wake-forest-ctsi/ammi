# extract the entire table from the datamart into a parquet file

import sqlalchemy
import os
from dotenv import load_dotenv
import pandas as pd

load_dotenv(override=True)

def consistency_check(dat):
    '''
      need to check how many features are common so the machine learning models are seeing the same features
    '''
    wfu_features = []
    with open("wfu_features.txt", "r") as fid:
        for line in fid:
            wfu_features.append(line.strip())
    
    # removing columns not in wfu dataset
    cols_to_delete = []
    for col in dat.columns:
        if (col not in wfu_features):
            print(f"{col} not in wfu dataset, removing it")
    dat.drop(columns = cols_to_delete, inplace=True)

    # adding columns not in your site
    for col in wfu_features:
        if (col not in dat.columns):
            print(f"WARNING: {col} not in your dataset, adding it and fill it with 0; this could be dangerous.")
            dat[col] = 0.0

if (__name__ == "__main__"):
    # this assumes using windows authentication
    # if you have pyodbc, use this
    # connection_string = f'mssql+pyodbc://@{os.getenv('HOSTNAME')}/{os.getenv('DATABASE')}?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server'
    # if you have pymssql, use this
    connection_string = f'mssql+pymssql://@{os.getenv('HOSTNAME')}/{os.getenv('DATABASE')}'
    engine = sqlalchemy.create_engine(connection_string)

    sql_string = f'''
    select
      *
    from {os.getenv('DATABASE')}.{os.getenv('SCHEMA')}.rpt_depression
    '''
    print("transfering data from database")
    dat = pd.read_sql(sql_string, con=engine)
    print(f"dat shape: rows={dat.shape[0]}, cols={dat.shape[1]}")

    # need to do a consistency check to see if all columns match
    consistency_check(dat)

    # create a train test split
    dat_test = dat.sample(frac=float(os.getenv('TEST_SIZE')), random_state=42)
    dat_train = dat.drop(dat_test.index).reset_index(drop=True)
    dat_test = dat_test.reset_index(drop=True)

    print(f"writing data to {os.getenv('TRAIN_PARQUET') and {os.getenv('TEST_PARQUET')}} with test_size={os.getenv('TEST_SIZE')}")
    dat_train.to_parquet(os.getenv('TRAIN_PARQUET'), index=False)
    dat_test.to_parquet(os.getenv('TEST_PARQUET'), index=False)
