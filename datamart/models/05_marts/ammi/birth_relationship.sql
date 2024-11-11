select
    *
from
    {{ source('pcornet', 'birth_relationship') }}