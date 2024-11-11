select
    *
from
    {{ source('pcornet', 'prescribing') }}