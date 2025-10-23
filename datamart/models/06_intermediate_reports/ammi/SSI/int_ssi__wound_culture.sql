with cohort as (
    select
        cohort.birthid,
        cohort.mother_patid,
        cohort.baby_birth_date
    from {{ ref('int_ssi__cohort') }} cohort
),

lab as (
    select
        patid,
        {{ add_time_to_date_macro("specimen_date", "specimen_time") }} as specimen_date,
        case when lower(raw_result) like 'normal%' then 0
             when lower(raw_result) like 'no growth' then 0
             else 1 end as wound_culture
    from {{ ref('lab_result_cm')}}
    where lab_loinc = '17915-0'
),

renamed as (
    select
        cohort.birthid,
        -- max(case when wound_culture is not null then wound_culture else 0 end) as wound_culture
        max(wound_culture) as wound_culture 
    from cohort
    left join lab on cohort.mother_patid = lab.patid
     and specimen_date between cohort.baby_birth_date and dateadd(day, 30, cohort.baby_birth_date)
    group by cohort.birthid
)

select * from renamed