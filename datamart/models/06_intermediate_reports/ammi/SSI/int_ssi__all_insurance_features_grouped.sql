select
    birthid,
    insurance as 'feature_name',
    1 as 'value'
from {{ ref('int_ssi__all_insurance_features') }}
group by birthid, insurance