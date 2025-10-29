{{ config(materialized='table') }}

with grouped_cte as (
    select
        birthid,
        rxnorm_cui,
        max(datediff(day, estimated_preg_start_date, rx_order_date)) as earliest_day,
        min(datediff(day, estimated_preg_start_date, rx_order_date)) as latest_day,
        1 as has_rx
    from {{ ref('int_preeclampsia__all_rx_features') }}
    group by birthid, rxnorm_cui
)

-- unpivot

select
    birthid,
    rxnorm_cui + '_earliest_day' as 'feature_name',
    earliest_day as 'value'
from grouped_cte

union all

select
    birthid,
    rxnorm_cui + '_latest_day' as 'feature_name',
    latest_day as 'value'
from grouped_cte

union all

select
    birthid,
    rxnorm_cui + '_has_rx' as 'feature_name',
    has_rx as 'value'
from grouped_cte