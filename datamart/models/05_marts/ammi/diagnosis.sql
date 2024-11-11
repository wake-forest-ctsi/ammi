select
    *
from
    {{ source('pcornet', 'diagnosis') }}