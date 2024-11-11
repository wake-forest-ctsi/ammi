
with source as (

    select * from {{ source('pcornet', 'diagnosis') }}

),

renamed as (

    select
        diagnosisid,
        patid,
        encounterid,
        enc_type,
        admit_date,
        providerid,
        dx,
        dx_type,
        dx_date,
        dx_source,
        dx_origin,
        pdx,
        dx_poa,
        raw_dx,
        raw_dx_type,
        raw_dx_source,
        raw_origdx,
        raw_pdx,
        raw_dx_poa

    from source

)

select * from renamed

