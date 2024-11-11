
with source as (

    select * from {{ source('pcornet', 'procedures') }}

),

renamed as (

    select
        proceduresid,
        patid,
        encounterid,
        enc_type,
        admit_date,
        providerid,
        px_date,
        px,
        px_type,
        px_source,
        ppx,
        raw_px,
        raw_px_type,
        raw_ppx

    from source

)

select * from renamed

