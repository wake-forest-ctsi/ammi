with grouped_cte as (
    select
        birthid,
        dx,
        max(datediff(day, dx_date, baby_birth_date)) as earliest_day,
        min(datediff(day, dx_date, baby_birth_date)) as latest_day,
        1 as has_dx
    from {{ ref('int_ssi__all_dx_features') }}
    group by birthid, dx
)

-- unpivot
select
    birthid,
    dx + '_earliest_day' as 'feature_name',
    earliest_day as 'value'
from grouped_cte

union all

select
    birthid,
    dx + '_latest_day' as 'feature_name',
    latest_day as 'value'
from grouped_cte

union all

select
    birthid,
    dx + '_has_dx' as 'feature_name',
    has_dx as 'value'
from grouped_cte