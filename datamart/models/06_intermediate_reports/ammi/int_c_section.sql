{{ config(materialized='table') }}

with cohort as (
    select
        *
    from {{ ref('int_cohort') }}
),

c_section as (
    select
        encounterid,
        min(raw_px) as delivery_mode
    from {{ ref('procedures') }}
    where raw_px like 'C-Section%'
    group by encounterid
),

renamed as (
    select
        cohort.birthid,
        c_section.delivery_mode
    from cohort
    inner join c_section on cohort.mother_encounterid = c_section.encounterid
)

select * from renamed