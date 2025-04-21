import pandas as pd

if (__name__ == "__main__"):
    dat = pd.read_parquet('depression_wfu.parquet')
    with open("wfu_features.txt", 'w') as fid:
        for col in dat.columns:
            fid.write(f"{col}\n")