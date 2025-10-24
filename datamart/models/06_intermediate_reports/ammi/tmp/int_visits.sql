with cohort as (
    select 
        birthid,
        mother_patid,
        estimated_preg_start_date,
        baby_birth_date
    from {{ ref('int_cohort') }}
),

-- it's better to use date only since there can be multiple entries for the save visit
encounter as (
    select
        patid,
        admit_date,
        enc_type,
        payer_type_primary
    from {{ ref('encounter') }}
),

renamed as (
    select
        a.birthid,
        b.admit_date,
        -- datediff(day, a.baby_birth_date, b.admit_date) as days_since_child_birth,
        b.enc_type,
        b.payer_type_primary
    from cohort a
    left join encounter b on a.mother_patid = b.patid
)

select * from renamed