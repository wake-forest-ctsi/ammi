{{ config(materialized='table') }}

select
    cohort.*
from {{ ref('int_cohort') }} cohort
inner join {{ ref('int_c_section') }} csection on cohort.birthid = csection.birthid