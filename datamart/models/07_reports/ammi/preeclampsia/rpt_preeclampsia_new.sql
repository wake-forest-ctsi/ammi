{% set tables = [
    {'description': 'phenotype', 'table': ref('int_preeclampsia__phenotype')},
    {'description': 'baseline', 'table': ref('int_preeclampsia__baseline_features')},
    {'description': 'diagnosis', 'table': ref('int_preeclampsia__all_dx_features_grouped')},
    {'description': 'prescribing', 'table': ref('int_preeclampsia__all_rx_features_grouped')},
    {'description': 'encounter_counts', 'table': ref('int_preeclampsia__all_enctype_features_grouped')},
    {'description': 'insurance', 'table': ref('int_preeclampsia__all_insurance_features_grouped')},
    {'description': 'vital', 'table': ref('int_preeclampsia__all_vital_features_grouped')}
] %}

{% for pair in tables %}
select 
    birthid,
    '{{ pair.description }}' as 'feature_category',
    feature_name,
    value as 'feature_value'
from {{ pair.table }}
{% if not loop.last %} union all {% endif %}
{% endfor %}

