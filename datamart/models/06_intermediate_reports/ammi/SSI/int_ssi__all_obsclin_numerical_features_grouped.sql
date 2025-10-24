with median_cte as (
    select
        distinct
        birthid,
        obsclin_code,
        percentile_cont(0.5) within group (order by result) over (partition by birthid, obsclin_code) as 'median_value'
    from {{ ref('int_ssi__all_obsclin_numerical_features') }}
),

other_stat_cte as (
    select
        birthid,
        obsclin_code,
        avg(result) as 'mean_value',
        min(result) as 'min_value',
        max(result) as 'max_value',
        count(result) as 'counts'
    from {{ ref('int_ssi__all_obsclin_numerical_features') }}
    group by birthid, obsclin_code
)

-- do the unpivot here

select
    birthid,
    obsclin_code + '_median' as 'feature_name',
    median_value as 'value'
from median_cte

union all

select
    birthid,
    obsclin_code + '_mean' as 'feature_name',
    mean_value as 'value'
from other_stat_cte

union all

select
    birthid,
    obsclin_code + '_min' as 'feature_name',
    min_value as 'value'
from other_stat_cte

union all

select
    birthid,
    obsclin_code + '_max' as 'feature_name',
    max_value as 'value'
from other_stat_cte

union all

select
    birthid,
    obsclin_code + '_counts' as 'feature_name',
    counts as 'value'
from other_stat_cte
