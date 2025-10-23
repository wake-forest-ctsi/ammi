select
    birthid,
    datediff(day, specimen_date, baby_birth_date) as specimen_day,
    lab_loinc,
    lab_name,
    lower(result) as result,
    row_number() over (partition by birthid, lab_loinc order by specimen_date desc) as rr
from {{ ref('int_ssi__all_lab_text_features') }}