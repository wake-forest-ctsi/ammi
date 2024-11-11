select
    *
from
    {{ source('pcornet', 'demographic') }}