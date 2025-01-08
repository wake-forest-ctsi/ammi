with cohort as (
    select 
        *
    from {{ ref('int_cohort') }}
),

mother_demographic as (
    select
        patid,
        birth_date
    from {{ ref('stg_pcornet__demographic') }}
),

renamed as (
    select
        a.birthid,
        datediff(year, b.birth_date, a.baby_birth_date) as mother_age
    from cohort a
    left join mother_demographic b on a.mother_patid = b.patid
)

select * from renamed