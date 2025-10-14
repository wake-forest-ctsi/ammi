with cohort as (
    select
        birthid,
        mother_encounterid
    from {{ ref('int_cohort') }}
),

renamed as (
    select
        cohort.birthid,
        encounter.enc_type,
        encounter.admitting_source
    from cohort
    left join encounter 
      on cohort.mother_encounterid = encounter.encounterid
)

select * from renamed