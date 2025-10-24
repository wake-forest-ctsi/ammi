with dummy_cte as (
    select
        birthid,
        datediff(day, obsclin_start_date, baby_birth_date) as obsclin_day,
        obsclin_code + '--' + lower(result) as obsclin_code_result,
        obsclin_name
    from {{ ref('int_ssi__all_obsclin_text_features') }}
),

grouped_cte as (
    select
        birthid,
        obsclin_code_result,
        max(obsclin_day) as earliest_day,
        min(obsclin_day) as latest_day,
        1 as has_obsclin_code_result
    from dummy_cte
    group by birthid, obsclin_code_result
)

-- do unpivot

select
    birthid,
    obsclin_code_result + '_earliest_day' as 'feature_name',
    earliest_day as 'value'
from grouped_cte

union all

select
    birthid,
    obsclin_code_result + '_latest_day' as 'feature_name',
    latest_day as 'value'
from grouped_cte

union all

select
    birthid,
    obsclin_code_result + '_has_obsclin_code_result' as 'feature_name',
    has_obsclin_code_result as 'value'
from grouped_cte
