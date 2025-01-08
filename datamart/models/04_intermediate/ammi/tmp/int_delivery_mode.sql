with cohort as (
    select
        *
    from {{ ref('int_cohort') }}
),

delivery_mode as (
    select
        encounterid,
        min(raw_px) as delivery_mode -- it's possible this mess up for twins
    from {{ ref('stg_pcornet__procedures') }}
    where px in ('177157003', '61586001', '48204000', '84195007', '11466000', '302383004') 
       or raw_px like 'C-Section%'
       or raw_px like 'Induction'
       or raw_px like 'VBAC%'
    group by encounterid
),

renamed as (
    select
        cohort.birthid,
        delivery_mode.delivery_mode
    from cohort
    left join delivery_mode on cohort.mother_encounterid = delivery_mode.encounterid
)

select * from renamed