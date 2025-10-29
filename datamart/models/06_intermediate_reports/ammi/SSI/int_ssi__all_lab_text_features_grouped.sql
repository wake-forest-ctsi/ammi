{{ config(materialized='table') }}

with dummy_cte as (
    select
        birthid,
        datediff(day, specimen_date, baby_birth_date) as specimen_day,
        lab_loinc + '--' + lower(result) as lab_loinc_result,
        lab_name
    from {{ ref('int_ssi__all_lab_text_features') }}
),

grouped_cte as (
    select
        birthid,
        lab_loinc_result,
        max(specimen_day) as earliest_day,
        min(specimen_day) as latest_day,
        1 as has_lab_loinc_result
    from dummy_cte
    group by birthid, lab_loinc_result
)

-- do unpivot

select
    birthid,
    lab_loinc_result + '_earliest_day' as 'feature_name',
    earliest_day as 'value'
from grouped_cte

union all

select
    birthid,
    lab_loinc_result + '_latest_day' as 'feature_name',
    latest_day as 'value'
from grouped_cte

union all

select
    birthid,
    lab_loinc_result + '_has_lab_loinc_result' as 'feature_name',
    has_lab_loinc_result as 'value'
from grouped_cte