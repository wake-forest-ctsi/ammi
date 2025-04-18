with cohort as (
    select 
        birthid,
        mother_patid,
        estimated_pregnancy_date,
        baby_birth_date
    from {{ ref('int_cohort') }}
),

encounter as (
    select 
        patid,
        {{ add_time_to_date_macro("admit_date", "admit_time") }} as admit_date,
        enc_type,
        payer_type_primary
    from {{ ref('encounter') }}
),

renamed as (
    select 
        a.birthid,
        b.admit_date,
        datediff(day, a.baby_birth_date, b.admit_date) as days_since_child_birth,
        b.enc_type,
        b.payer_type_primary
    from cohort a
    left join encounter b on a.mother_patid = b.patid
)

select * from renamed