with median_cte as (
    select
        birthid,
        obsclin_code,
        percentile_cont(0.5) within group (order by result) over (partition by birthid, obsclin_code) as median_value,
        result
    from {{ ref('int_ssi__all_obsclin_numerical_features') }}
),

renamed as (
    select
        birthid,
        obsclin_code,
        max(median_value) as median_value,  -- median is the same, just pick one
        min(result) as min_value,
        max(result) as max_value,
        avg(result) as mean_value,
        count(1) as counts
    from median_cte
    group by birthid, obsclin_code
)

select * from renamed