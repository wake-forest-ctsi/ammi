with all_dates as (
    select
       a.birthid,
       a.motherid,
       a.mother_encounterid,
       {{ add_time_to_date_macro("b.birth_date", "b.birth_time") }} as baby_birth_date,
       {{ add_time_to_date_macro("c.admit_date", "c.admit_time") }} as delivery_admit_date,
       {{ add_time_to_date_macro("c.discharge_date", "c.discharge_time") }} as delivery_discharge_date,
       dateadd(day, -d.gest_age_in_days, {{ add_time_to_date_macro("b.birth_date", "b.birth_time") }}) as estimated_pregnancy_date
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