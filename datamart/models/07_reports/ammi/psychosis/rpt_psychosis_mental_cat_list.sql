with mental_cat as (
    select
        *
    from {{ ref('int_mental_cat') }}
),

condition_grouped as (
    select
        *
    from {{ ref('int_condition_grouped') }}
),

renamed as (
    select
        mental_cat.*,
        condition_grouped.chief_complaint,
        condition_grouped.health_problem_list,
        condition_grouped.pcornet_defined_list
    from mental_cat
    left join condition_grouped on mental_cat.encounterid = condition_grouped.encounterid
)

select * from renamed