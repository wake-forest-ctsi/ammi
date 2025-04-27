select
    birthid,
    case when mother_age <= 17 then 17
         when mother_age >= 45 then 45
         else mother_age end as mat_age_pu
from {{ ref('int_mother_age_at_birth') }}