{{ config(materialized='table') }}

with median_cte as (
    select
        distinct
        birthid,
        lab_loinc,
        percentile_cont(0.5) within group (order by result) over (partition by birthid, lab_loinc) as 'median_value'
    from {{ ref('int_ssi__all_lab_numerical_features') }}
),

other_stat_cte as (
    select
        birthid,
        lab_loinc,
        avg(result) as 'mean_value',
        min(result) as 'min_value',
        max(result) as 'max_value',
        count(1) as 'counts'
    from {{ ref('int_ssi__all_lab_numerical_features') }}
    group by birthid, lab_loinc
)

-- do the unpivot here

select
    birthid,
    lab_loinc + '_median' as 'feature_name',
    median_value as 'value'
from median_cte

union all

select
    birthid,
    lab_loinc + '_mean' as 'feature_name',
    mean_value as 'value'
from other_stat_cte

union all

select
    birthid,
    lab_loinc + '_min' as 'feature_name',
    min_value as 'value'
from other_stat_cte

union all

select
    birthid,
    lab_loinc + '_max' as 'feature_name',
    max_value as 'value'
from other_stat_cte

union all

select
    birthid,
    lab_loinc + '_counts' as 'feature_name',
    counts as 'value'
from other_stat_cte
