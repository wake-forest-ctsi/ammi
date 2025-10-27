{{ config(materialized='table') }}

select
    birthid,
    enc_type + '_during_pregnancy_count' as 'feature_name',
    count(1) as 'value'
from {{ ref('int_preeclampsia__all_enctype_features') }}
group by birthid, enc_type

union all

select
    birthid,
    'total_visits_during_pregnancy_count' as 'feature_name',
    count(1) as 'value'
from {{ ref('int_preeclampsia__all_enctype_features') }}
group by birthid