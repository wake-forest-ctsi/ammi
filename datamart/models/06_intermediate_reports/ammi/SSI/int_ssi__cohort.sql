with csection as (
    select
        patid,
        encounterid,
        enc_type,
        admit_date,
        px_date,
        raw_px
    from {{ ref('procedures') }}
    where raw_px like 
),

delivery_dates as (
    select
       birthid,
       mother_patid,
       mother_encounterid,
       baby_birth_date,
       delivery_admit_date,
       delivery_discharge_date
    from {{ ref('int_cohort') }} birth_relationship
    inner join {{ ref('demographic') }} demographic on birth_relationship.patid = demographic.patid
    inner join {{ ref('encounter') }} encounter on birth_relationship.mother_encounterid = encounter.encounterid
    where demographic.birth_date is not null 
      and encounter.admit_date is not null
),

-- the delivery mode is accurate to encounter level, so better remove twins
twins as (
    select
        distinct(d1.birthid)
    from delivery_dates d1
    left join delivery_dates d2 on d1.motherid = d2.motherid
    where d1.birthid != d2.birthid
      and abs(datediff(day, d2.baby_birth_date, d1.baby_birth_date)) < 10
)

select * from twins