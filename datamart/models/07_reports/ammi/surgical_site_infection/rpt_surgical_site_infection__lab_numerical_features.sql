with median_cte as (
    select
        birthid,
        lab_loinc,
        percentile_cont(0.5) within group (order by result) over (partition by birthid, lab_loinc) as median_value,
        result
    from {{ ref('int_ssi__all_lab_numerical_features') }}
),

renamed as (
    select
        birthid,
        lab_loinc,
        max(median_value) as median_value,
        min(result) as min_value,
        max(result) as max_value,
        avg(result) as mean_value,
        count(1) as counts
    from median_cte
    group by birthid, lab_loinc
)

select * from renamed