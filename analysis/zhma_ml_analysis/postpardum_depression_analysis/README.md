# Project Overview

This project demonstrates the steps needed to train or test using the data extracted from PCORnet using the dbt tools in `../datamart`. The focus is on post-partum depression.

### Prerequisites 

All packages information is stored in the `requirements.txt`. You can run this to install all packages:
   ```bash
   pip install -r requirements.txt
   ```

### Running the Project

#### 1. Extract data into a Parquet file:

This step is required whether you want to train your own model or only test the WFU model on your dataset. Ideally, this command will extract contents from your post-partum depression fact table (`rpt_depression`):
   ```bash
   python extract.py
   ```

The script reads the connection information from your `.env` file. Examples of `.env` files for training and testing only are included. The current setup uses MS SQL Server with Windows authentication. You may need to modify the `connection_string` line in `extract.py` to suit your environment.

Additionally, the script performs a consistency check with WFU features to ensure all features are identical across sites, which is essential for running the same ML model.

#### 2. Train your model using your own data:

The `train.ipynb` notebook provides steps to train your model using your site's data. You may need to modify the following variables in your `.env` file:
- `TEST_SIZE`
- `TRAIN_PARQUET`
- `TEST_PARQUET`
- `ML_MODEL`
- `NUM_CPUS`

Refer to `env.train_example` for an example. The training process is based on a Random Forest model. We also output model performance across different racial groups for comparison.

#### 3. Test the WFU model on your own data:

If you are only interested in testing the WFU model on your data, you can use `test.ipynb`. In this case, you will likely set these in your `.env` file:
- `TEST_SIZE=1.0` to use all your data.
- `ML_MODEL=depression_ml_model_wfu.pickle` to use the WFU model.

See `env.test_only_example` for more details.

#### 4. preprocess.py

All preprocessing steps are implemented in `preprocess.py`. This function is called within both `train.ipynb` and `test.ipynb`, so it does not need to be run separately. Please review the script to see if any changes are necessary.

### Known Limitations

All scripts and the final ML model required to reproduce the results in our paper will be uploaded once finalized.