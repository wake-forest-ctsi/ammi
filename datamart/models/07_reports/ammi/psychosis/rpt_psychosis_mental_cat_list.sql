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
        a.*,
        b.chief_complaint,
        b.health_problem_list,
        b.pcornet_defined_list
    from mental_cat a
    left join condition_grouped b on a.encounterid = b.encounterid
)

select * from renamed