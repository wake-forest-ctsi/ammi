select
    birthid,
    mother_patid,
    baby_birth_date,
    delivery_admit_date,
    delivery_discharge_date,
    obsclin_start_date,
    gestage_days,
    obsclin_code,
    phq_value
from {{ ref('int_phq_epds') }}