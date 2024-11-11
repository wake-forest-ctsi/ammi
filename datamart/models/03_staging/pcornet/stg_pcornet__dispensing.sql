
with source as (

    select * from {{ source('pcornet', 'dispensing') }}

),

renamed as (

    select
        dispensingid,
        patid,
        prescribingid,
        dispense_date,
        ndc,
        dispense_source,
        dispense_sup,
        dispense_amt,
        dispense_dose_disp,
        dispense_dose_disp_unit,
        dispense_route,
        raw_ndc,
        raw_dispense_dose_disp,
        raw_dispense_dose_disp_unit,
        raw_dispense_route

    from source

)

select * from renamed

