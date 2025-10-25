-- 3 filters: has at least 1 prenatal visit, 1 postpartum visit (within 3 months) and birth_date < '2023'05-31'

{{ config(materialized='table') }}

with visit_flags as (
    select
        birthid,
        max(case when days_since_child_birth between -268 and -1 then 1 else 0 end) as has_prenatal_visit,
        max(case when days_since_child_birth between 1 and 90 then 1 else 0 end) as has_postpartum_visit
    from {{ ref('int_visits') }}
    group by birthid
),

filtered_births as (
    select
        birthid
    from visit_flags
    where has_prenatal_visit = 1 and has_postpartum_visit = 1
)

select
    cohort.*
from {{ ref('int_cohort') }} cohort
inner join filtered_births on cohort.birthid = filtered_births.birthid
where cohort.baby_birth_date < '2023-05-31'
