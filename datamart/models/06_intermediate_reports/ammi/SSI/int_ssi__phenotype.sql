{{ config(materialized='table') }}

with cohort as (
    select
        birthid,
        mother_patid,
        baby_birth_date
    from {{ ref('int_ssi__cohort') }}
),

diagnosis as (
    select
        patid,
        dx_date,
        dx
    from {{ ref('diagnosis')}}
    where dx like 'O86.0%' or dx like 'T81.4%'
),

wound_culture as (
    select
        birthid,
        wound_culture
    from {{ ref('int_ssi__wound_culture') }}
),

all_posibilities as (
    select
        cohort.birthid,
        max(case when dx is not null then 1 else 0 end) as SSI_diagnosis,
        max(case when wound_culture.wound_culture is not null then wound_culture else 0 end) as wound_culture
    from cohort
    left join diagnosis on cohort.mother_patid = diagnosis.patid
     and datediff(day, cohort.baby_birth_date, dx_date) <= 30
     and datediff(day, cohort.baby_birth_date, dx_date) >= 0
    left join wound_culture on cohort.birthid = wound_culture.birthid
    group by cohort.birthid
)

select
    birthid,
    'target_variable' as 'feature_name',
    case when (SSI_diagnosis = 1) or (wound_culture) = 1 then 1
         else 0 end as 'value'
from all_posibilities