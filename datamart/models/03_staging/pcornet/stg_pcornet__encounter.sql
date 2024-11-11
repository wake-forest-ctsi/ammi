
with source as (

    select * from {{ source('pcornet', 'encounter') }}

),

renamed as (

    select
        patid,
        encounterid,
        admit_date,
        admit_time,
        discharge_date,
        discharge_time,
        providerid,
        facility_location,
        enc_type,
        facilityid,
        discharge_disposition,
        discharge_status,
        drg,
        drg_type,
        admitting_source,
        payer_type_primary,
        payer_type_secondary,
        facility_type,
        raw_siteid,
        raw_enc_type,
        raw_discharge_disposition,
        raw_discharge_status,
        raw_drg_type,
        raw_admitting_source,
        raw_facility_type,
        raw_payer_type_primary,
        raw_payer_name_primary,
        raw_payer_id_primary,
        raw_payer_type_secondary,
        raw_payer_name_secondary,
        raw_payer_id_secondary

    from source

)

select * from renamed

