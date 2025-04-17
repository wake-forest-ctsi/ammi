select
    birthid,
    patid,
    encounterid,
    pregnancyid,
    motherid,
    mother_encounterid
from
    {{ source('pcornet', 'birth_relationship') }}