select
    *
from
    {{ source('pcornet', 'lab_result_cm') }}