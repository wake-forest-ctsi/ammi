select
    patid,
    cast(birth_date as date) birth_date,
    birth_time,
    sex,
    sexual_orientation,
    gender_identity,
    hispanic,
    biobank_flag,
    race,
    pat_pref_language_spoken,
    raw_sex,
    raw_hispanic,
    raw_race,
    raw_sexual_orientation,
    raw_gender_identity,
    raw_pat_pref_language_spoken
from
    {{ source('pcornet', 'demographic') }}