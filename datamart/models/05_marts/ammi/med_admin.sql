select
    *
from
    {{ source('pcornet', 'med_admin') }}