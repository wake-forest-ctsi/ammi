with c_section as (
    select
        patid,
        encounterid as c_section_encounterid,
        min(enc_type) as c_section_enc_type,
        min(admit_date) as c_section_admit_date,
        min(px_date) as c_section_px_date
    from {{ ref('procedures') }}
    where raw_px like 'C-Section%' and px_date is not null
    group by patid, encounterid
),

all_dates as (
    select
       a.birthid,
       a.motherid,
       a.mother_encounterid,
       {{ add_time_to_date_macro("b.birth_date", "b.birth_time") }} as baby_birth_date,
       {{ add_time_to_date_macro("c.admit_date", "c.admit_time") }} as delivery_admit_date,
       {{ add_time_to_date_macro("c.discharge_date", "c.discharge_time") }} as delivery_discharge_date,
       dateadd(day, -d.gest_age_in_days, {{ add_time_to_date_macro("b.birth_date", "b.birth_time") }}) as estimated_preg_start_date,
       e.c_section_encounterid,
       -- e.c_section_enc_type,
       -- e.c_section_admit_date,
       e.c_section_px_date
    from {{ ref('birth_relationship') }} a
    inner join {{ ref('demographic') }} b on a.patid = b.patid
    inner join {{ ref('encounter') }} c on a.mother_encounterid = c.encounterid
    inner join {{ ref('int_gestational_age') }} d on a.birthid = d.birthid
    -- i think it's okay to just join the encounterid, but just to be safe
    left join c_section e on a.motherid = e.patid and abs(datediff(day, b.birth_date, e.c_section_px_date)) < 10  
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
        estimated_preg_start_date,
        c_section_encounterid,
        -- c_section_enc_type,
        -- c_section_admit_date,
        c_section_px_date
    from all_dates
)

select * from renamed