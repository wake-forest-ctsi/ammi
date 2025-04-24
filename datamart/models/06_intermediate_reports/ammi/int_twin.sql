with cohort as (
    select
        *
    from {{ ref('int_cohort') }}
),

birth_counts as (
    select
        mother_encounterid,
        count(*) as birth_counts
    from {{ ref('birth_relationship') }}
    group by mother_encounterid
),

renamed as (
    select
        cohort.birthid,
        birth_counts.birth_counts
    from cohort 
    left join birth_counts on cohort.mother_encounterid = birth_counts.mother_encounterid
)

select * from renamed