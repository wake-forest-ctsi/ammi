select
    proceduresid,
    patid,
    encounterid,
    enc_type,
    cast(admit_date as date) admit_date,
    providerid,
    cast(px_date as date) px_date, -- there's only date here anyway
    px,
    px_type,
    px_source,
    ppx,
    raw_px,
    raw_px_type,
    raw_ppx
from {{ ref("stg_pcornet__procedures") }}