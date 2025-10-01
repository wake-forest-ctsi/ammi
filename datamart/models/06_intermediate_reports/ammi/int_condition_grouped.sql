with condition_grouped as (
    select
        encounterid,
        condition_source,
        string_agg(condition, ', ') as condition
    from {{ ref('condition') }}
    group by encounterid, condition_source
),

renamed as (
    select
        encounterid,
        min(case when condition_source = 'CC' then condition else null end) as chief_complaint,
        min(case when condition_source = 'HC' then condition else null end) as health_problem_list,
        min(case when condition_source = 'PC' then condition else null end) as pcornet_defined_list
    from condition_grouped
    group by encounterid
)

select * from renamed