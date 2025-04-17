select
    diagnosisid,
    patid,
    encounterid,
    enc_type,
    cast(admit_date as date) admit_date,
    providerid,
    dx,
    dx_type,
    cast(dx_date as date) dx_date,
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
from
    {{ source('pcornet', 'diagnosis') }}