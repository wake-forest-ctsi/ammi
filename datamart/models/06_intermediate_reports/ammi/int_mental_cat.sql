with cohort as (
    select
        birthid,
        mother_patid,
        estimated_preg_start_date,
        baby_birth_date,
        delivery_admit_date,
        delivery_discharge_date
    from {{ ref('int_cohort') }}
),

dx_list as (
    select
        diagnosis.diagnosisid,
        diagnosis.patid,
        diagnosis.dx_date,
        diagnosis.dx,
        diagnosis.encounterid,
        diagnosis.enc_type,
        diagnosis.pdx,
        mental_cat_list.mental_cat
    from {{ ref('diagnosis') }} diagnosis
    inner join {{ ref('mental_cat')}} mental_cat_list
      on (mental_cat_list.match_type = 'exact' and diagnosis.dx = mental_cat_list.dx)
    
    union all

    select
        diagnosis.diagnosisid,
        diagnosis.patid,
        diagnosis.dx_date,
        diagnosis.dx,
        diagnosis.encounterid,
        diagnosis.enc_type,
        diagnosis.pdx,
        mental_cat_list.mental_cat
    from {{ ref('diagnosis') }} diagnosis
    inner join {{ ref('mental_cat')}} mental_cat_list
      on (mental_cat_list.match_type = 'like' and diagnosis.dx like mental_cat_list.dx)

),

renamed as (
    select
        cohort.birthid,
        cohort.mother_patid,
        cohort.baby_birth_date,
        cohort.delivery_admit_date,
        cohort.delivery_discharge_date,
        -- dx_list.diagnosisid,  -- for debugging only
        dx_list.dx_date,
        dx_list.dx,
        dx_list.encounterid,
        dx_list.enc_type,
        dx_list.pdx,
        datediff(day, cohort.estimated_preg_start_date, dx_list.dx_date) as gestage_days,
        dx_list.mental_cat
    from cohort
    left join dx_list on cohort.mother_patid = dx_list.patid
)

select * from renamed