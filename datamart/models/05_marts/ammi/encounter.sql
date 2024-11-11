select
    *
from
    {{ source('pcornet', 'encounter') }}