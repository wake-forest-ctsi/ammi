# Project Overview

This project builds tables with `birth_id` as the grain, designed for machine learning analysis. Currently, it supports tables for preeclampsia and postpartum depression, with plans to extend to include cesarean section data in the future.


### Running the Project

#### If You Only Need a Table in the SQL Server:

1. **Seed the data**: This command refreshes any existing seeds.
   ```bash
   dbt seed --full-refresh
   ```

2. **Build the report table** (e.g., `rpt_depression` for postpartum depression):
   ```bash
   dbt run --select +rpt_depression --vars "{'report': 'depression'}" --full-refresh
   ```

#### If You Need a Parquet File (for easier I/O like myself):

For those who prefer to work with Parquet files rather than database views, a Python script is provided.

1. **Run the Python script**:
   ```bash
   python extract.py --target dev --model rpt_depression --report depression --output_parquet depression.parquet
   ```

This will generate a Parquet file from the selected report. It will also remove all the intermediate views in the database and keeping only the table for the report model.

### Files That Need Some Explanations

#### 1. `seeds/daterange.csv`

In this project, reports may rely on different start and end dates for feature and target variables for different research questions. These date ranges are stored in the `daterange.csv` seed file. The date ranges are determined based on the report specified in the dbt command (via `var("report")`). If no date range is found for a specific model, it defaults to the `int_cohort` model for that report.

The date range is then passed to different models through the `get_date_range` macro. You can find its implementation in the `macros/get_date_range.sql` file.

#### 2. `03_staging/pcornet/base_pcornet__vital`

For the vital table, I added the `measure_time` to the `measure_date` field. Unlike other tables, the vital table in the AMMI database does not include time information in the `measure_date` field. This information is essential when calculating hypertension.

#### 3. `04_intermediate/int_censustract__svi_2022_zipcode`

This model computes data from the `stg_censustract__svi_2022_tract` table by mapping census tracts to zip codes. The mapping data was downloaded from [here](https://www.huduser.gov/portal/datasets/usps_crosswalk.html).

#### 4. `requirements.txt`

If you're only using views/tables, you only need the `dbt-core` package.

### Known Limitations

- **Recomputation for Reporting Models**:  
  The intermediate models are designed to be recomputed for each reporting model. As a result, I need to run `dbt run` for each specific report, which forces me to materialize the reporting models as tables. This approach differs from typical dbt projects, where a single `dbt run` can process all models. In this case, I cannot simply execute `dbt run` once across the entire project, as the vars are tailored to individual reports.

- **Reusability Across Time Periods**:  
  There isn't an easy way to reuse the same code for different time periods within the same report. For example, the models `int_bp_features_lifetime` and `int_vital_features` are essentially the same, but I have to write the same codes twice. 

- **dx and rx features**:
  Currently the dx and rx features are only useful for postpartum depression. Need to redo the search if need to apply for other phenotypes.

## Additional Resources

- **[dbt Documentation](https://docs.getdbt.com/docs/introduction)**: Official documentation for dbt.
- **[Discourse](https://discourse.getdbt.com/)**: Forum for commonly asked questions and answers.
- **[Slack Chat](https://community.getdbt.com/)**: Join the live discussions and get support.
- **[dbt Events](https://events.getdbt.com)**: Find upcoming dbt events near you.
- **[dbt Blog](https://blog.getdbt.com/)**: Stay up-to-date with dbt's development and best practices.

NOTE: this readme was written with some help from chatgpt.