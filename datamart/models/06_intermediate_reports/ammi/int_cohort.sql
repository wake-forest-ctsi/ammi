-- depends_on: {{ ref('daterange') }}

{% set date_range_list = get_date_range('int_cohort') %}

with all_dates as (
    select
       a.birthid,
       a.motherid,
       a.mother_encounterid,
       b.birth_date as baby_birth_date, -- this already has hh:mm:ss in it
       c.admit_date as delivery_admit_date, -- this already has hh:mm:ss in it
       c.discharge_date as delivery_discharge_date, -- this already has hh:mm:ss in it
       dateadd(day, -d.gest_age_in_days, b.birth_date) as estimated_pregnancy_date
    from {{ ref('stg_pcornet__birth_relationship') }} a
    inner join {{ ref('stg_pcornet__demographic') }} b on a.patid = b.patid
    inner join {{ ref('stg_pcornet__encounter') }} c on a.mother_encounterid = c.encounterid
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
    {{ date_range_list[2] }}
)

select * from renamed