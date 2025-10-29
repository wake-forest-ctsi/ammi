{{ config(materialized='table') }}

with grouped_cte as (
    select
        birthid,
        max(smoking) as smoking,
        max(tobacco) as tobacco
    from {{ ref('int_depression__all_smoking_features') }}
    group by birthid
)

-- unpivot

select
    birthid,
    'smoking' as 'feature_name',
    smoking as 'value'
from grouped_cte

union all

select
    birthid,
    'tobacco' as 'feature_name',
    tobacco as 'value'
from grouped_cte