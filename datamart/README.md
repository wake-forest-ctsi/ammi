# Project Overview

This project builds tables with `birthid` as the grain, designed for machine learning analysis. Currently, it supports tables for preeclampsia, postpartum depression and surgical site infection, with plans to extend to include postpartum hemorrhage in the future.

This has been redesigned. The current implementation reads all available source tables and produces a comprehensive, long-format report table for each phenotype. This approach lets the machine learning pipeline to automatically select the most relevant features, minimizing manual effort when introducing new phenotypes.

### Running the Project

This command will build the depression, preeclampsia and surgical infection models. It may take 15-30 minutes depending on the size.
   ```bash
   dbt run --full-refresh
   ```

### Example: Preeclampsia Workflow

There are two main routes to generate the final report tables (currently named `rpt_preeclampsia_new` to preserve older versions for comparison):

#### 1. Models dependent on study period
These include features such as diagnoses, insurance, prescriptions and etc. Each model uses a macro to filter by study period, then joins to the cohort, and finally aggregates by `birthid`.

**Example steps:**
- Start with `int_preeclampsia__cohort`, which has been materialized into a table to speed up following queries
- `int_preeclampsia__all_dx_features` calls the macro that joins all diagnosis data within the study period with the cohort
- `int_preeclampsia__all_dx_features_grouped` performs a `group by birthid, dx` then unpivot to a long table

#### 2. Models independent of study period
These include baseline features like mother's race, mother's height, parity and etc. These models are shared across all phenotypes and calculated only once.

**Example steps:**
- `int_race` and `int_mother_height` feed into `int_preeclampsia__baseline_features`, which are then inner joined with `int_preeclampsia__cohort`

The final `rpt_preeclampsia_new` table is the union of all these models.  
All `_grouped` models are materialized to estimate the time required for each step.


### Files That Need Some Explanations

#### 1. `marcos`

In this project, reports may rely on different start and end dates for feature and target variables for different research questions. To reduce code redundancies, we use macros to store the same calculations, and call the macros with different start and end dates in the relevant sqls.

#### 2. `05_marts/ammi`

All the dates now only have the date part to be consistent with the definition in PCORnet. To get the full time, we need to add the time part to the date part.

## Additional Resources

- **[dbt Documentation](https://docs.getdbt.com/docs/introduction)**: Official documentation for dbt.
- **[Discourse](https://discourse.getdbt.com/)**: Forum for commonly asked questions and answers.
- **[Slack Chat](https://community.getdbt.com/)**: Join the live discussions and get support.
- **[dbt Events](https://events.getdbt.com)**: Find upcoming dbt events near you.
- **[dbt Blog](https://blog.getdbt.com/)**: Stay up-to-date with dbt's development and best practices.