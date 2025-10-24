with dummy as (
    select
        birthid,
        datediff(day, obsclin_start_date, baby_birth_date) as obsclin_day,
        obsclin_code + '--' + lower(result) as obsclin_code_result,
        obsclin_name
    from {{ ref('int_ssi__all_obsclin_text_features') }}
),

renamed as (
    select
        birthid,
        obsclin_code_result,
        max(obsclin_day) as earliest_day,
        min(obsclin_day) as latest_day,
        1 as has_obsclin_code_result
    from dummy
    group by birthid, obsclin_code_result
)

select * from renamed