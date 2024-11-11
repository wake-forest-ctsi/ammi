select
    *
from
    {{ source('pcornet', 'obs_clin') }}