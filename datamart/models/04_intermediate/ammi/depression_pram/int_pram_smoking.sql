with cohort as (
    select 
        *
    from {{ ref('int_cohort') }}
),

smoking as (
    select
        patid,
        measure_date,
        case when smoking in ('01','02','05','07','08') then 1 else 0 end as currently_smoking,
        case when smoking = '01' or smoking = '07' then 1 else 0 end as heavy_smoking,
        case when smoking = '04' then 1 else 0 end as never_smoking
    from {{ ref('stg_pcornet__vital') }}
    where smoking in ('01', '02', '03', '04', '05', '07', '08')
),

smk_last2yrs as (
    select
        a.birthid,
        max(currently_smoking) as smoking_last2yrs
    from cohort a
    left join smoking b on a.mother_patid = b.patid
     and b.measure_date between dateadd(year, -2, a.baby_birth_date) and a.baby_birth_date
    group by a.birthid
),

smk_now as (
    select
        a.birthid,
        max(currently_smoking) as smoking_now,
        max(heavy_smoking) as heavy_smoking_now,
        (case when avg(never_smoking) = 1 then 1 else 0 end) as for_sure_never_smoking -- requires all to be 04 to get avg = 1
    from cohort a
    left join smoking b on a.mother_patid = b.patid
     and b.measure_date between a.baby_birth_date and dateadd(year, +1, a.baby_birth_date)
    group by a.birthid    
),

smk_last3month as (
    select
        a.birthid,
        max(currently_smoking) as smoking_last3month,
        max(heavy_smoking) as heavy_smoking_last3month
    from cohort a
    left join smoking b on a.mother_patid = b.patid
     and b.measure_date between dateadd(month, -3, a.baby_birth_date) and a.baby_birth_date
    group by a.birthid
),

renamed as (
    select
        smk_last2yrs.birthid,
        smoking_last2yrs + 1 as 'smk2yrs', -- this can be null
        smoking_now + 1 as 'smk6nw_a', -- this can be null
        smoking_last3month,
        smoking_now,
        heavy_smoking_last3month,
        heavy_smoking_now,
        case
          when smoking_last3month is null and for_sure_never_smoking = 1 then 1  -- only exception for null
          when smoking_last3month is null or smoking_now is null then null
          when smoking_last3month = 0 and smoking_now = 0 then 1
          when smoking_last3month = 1 and smoking_now = 0 then 2
          when smoking_last3month = 0 and smoking_now = 1 then 5
          -- smoking_last3month = 1 and smoking_now = 1 after reaching here
          when heavy_smoking_last3month > heavy_smoking_now then 3
          when heavy_smoking_last3month <= heavy_smoking_now then 4
          else '-999' end as 'smk6c_nw' -- should never get here
    from smk_last2yrs
    left join smk_now on smk_last2yrs.birthid = smk_now.birthid
    left join smk_last3month on smk_last2yrs.birthid = smk_last3month.birthid
)

select * from renamed