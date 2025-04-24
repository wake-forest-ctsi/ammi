{% macro mental_disorder_macro(date1, date2) %}

with cohort as (
    select 
        *
    from {{ ref('int_cohort') }}
),

diagnosis as (
    select
        patid,
        left(dx, 3) as dx, -- taken only the 3 letters from diagnosis code
        dx_date   -- dx has only date
    from {{ ref('diagnosis') }}
    where dx like 'F%' -- all mental diagnosis
       or dx like 'O99.34%' -- -- Other mental disorders complicating pregnancy, childbirth, and the puerperium
       or dx = 'Z79.899' -- Other long term (current) drug therapy
       or dx = 'Z86.59' -- Personal history of other mental and behavioral disorders
       or dx = '311' -- ICD 9 code for depression
       or dx = 'O09.9%' -- Supervision of high risk pregnancy, unspecified
),

renamed as (
    select
        cohort.birthid,
        cohort.baby_birth_date,
        diagnosis.dx,
        diagnosis.dx_date
    from cohort
    left join diagnosis on cohort.mother_patid = diagnosis.patid
     and diagnosis.dx_date between date1 and date2
)

select * from renamed

{% endmacro %}