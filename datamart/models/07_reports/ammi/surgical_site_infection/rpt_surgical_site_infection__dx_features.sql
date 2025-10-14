select
    birthid,
    dx,
    max(datediff(day, dx_date, baby_birth_date)) as earliest_day,
    min(datediff(day, dx_date, baby_birth_date)) as latest_day,
    1 as has_dx
from {{ ref('int_ssi__all_dx_features') }}
group by birthid, dx