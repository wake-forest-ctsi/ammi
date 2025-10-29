select
    birthid,
    patid,
    encounterid,
    pregnancyid,
    motherid,
    mother_encounterid
from
    {{ ref('stg_pcornet__birth_relationship') }}