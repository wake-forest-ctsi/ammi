select
    *
from
    {{ source('pcornet', 'vital') }}