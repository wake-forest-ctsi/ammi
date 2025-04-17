with all_dates as (
    select
       a.birthid,
       a.motherid,
       a.mother_encounterid,
       (cast(b.birth_date as datetime)+ cast(b.birth_time as datetime)) as baby_birth_date,
       (cast(c.admit_date as datetime)+ cast(c.admit_time as datetime)) as delivery_admit_date,
       (cast(c.discharge_date as datetime) + cast(c.discharge_time as datetime)) as delivery_discharge_date,
       dateadd(day, -d.gest_age_in_days, cast(b.birth_date as datetime) + cast(b.birth_time as datetime)) as estimated_pregnancy_date
    from {{ ref('birth_relationship') }} a
    inner join {{ ref('demographic') }} b on a.patid = b.patid
    inner join {{ ref('encounter') }} c on a.mother_encounterid = c.encounterid
    inner join {{ ref('int_gestational_age') }} d on a.birthid = d.birthid
    where b.birth_date is not null 
      and c.admit_date is not null
      and d.gest_age_in_days is not null
),

renamed as (
    select 
        birthid,
        motherid as mother_patid,
        mother_encounterid,
        baby_birth_date,
        delivery_admit_date,
        delivery_discharge_date,
        estimated_pregnancy_date
    from all_dates
)

select * from renamed