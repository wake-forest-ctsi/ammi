
with source as (

    select * from {{ source('pcornet', 'immunization') }}

),

renamed as (

    select
        immunizationid,
        patid,
        encounterid,
        proceduresid,
        vx_providerid,
        vx_record_date,
        vx_admin_date,
        vx_code_type,
        vx_code,
        vx_status,
        vx_status_reason,
        vx_source,
        vx_dose,
        vx_dose_unit,
        vx_route,
        vx_body_site,
        vx_manufacturer,
        vx_lot_num,
        vx_exp_date,
        raw_vx_name,
        raw_vx_code,
        raw_vx_code_type,
        raw_vx_dose,
        raw_vx_dose_unit,
        raw_vx_route,
        raw_vx_body_site,
        raw_vx_status,
        raw_vx_status_reason,
        raw_vx_manufacturer

    from source

)

select * from renamed

