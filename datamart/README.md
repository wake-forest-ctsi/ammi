# Project Overview

This project builds tables with `birth_id` as the grain, designed for machine learning analysis. Currently, it supports tables for preeclampsia and postpartum depression, with plans to extend to include cesarean section data in the future.


### Running the Project

#### If You Only Need a Table in the SQL Server:

1. **Seed the data**: This command refreshes any existing seeds.
   ```bash
   dbt seed --full-refresh
   ```

2. **Build the report table**: This command will build both the depression and preeclampsia models. It may take 5-10 minutes depending on the size.
   ```bash
   dbt run --full-refresh
   ```

### Files That Need Some Explanations

#### 1. `marcos`

In this project, reports may rely on different start and end dates for feature and target variables for different research questions. To reduce code redundancies, we use macros to store the same calculations, and call the macros with different start and end dates in the relevant sqls.

#### 2. `05_marts/ammi`

All the dates now only have the date part to be consistent with the definition in PCORnet. To get the full time, we need to add the time part to the date part.

#### 3. `05_marts/ammi/svi_2022_zipcode`

This model computes data from the `stg_censustract__svi_2022_tract` table by mapping census tracts to zip codes. The mapping data was downloaded from [here](https://www.huduser.gov/portal/datasets/usps_crosswalk.html).

### Known Limitations

- **dx and rx features**:
  Currently the dx and rx features are only useful for postpartum depression. Need to redo the search if need to apply for other phenotypes.

## Additional Resources

- **[dbt Documentation](https://docs.getdbt.com/docs/introduction)**: Official documentation for dbt.
- **[Discourse](https://discourse.getdbt.com/)**: Forum for commonly asked questions and answers.
- **[Slack Chat](https://community.getdbt.com/)**: Join the live discussions and get support.
- **[dbt Events](https://events.getdbt.com)**: Find upcoming dbt events near you.
- **[dbt Blog](https://blog.getdbt.com/)**: Stay up-to-date with dbt's development and best practices.