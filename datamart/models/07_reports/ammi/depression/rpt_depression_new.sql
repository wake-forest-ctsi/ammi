{% set tables = [
    {'description': 'phenotype', 'table': ref('int_depression__phenotype')},
    {'description': 'baseline', 'table': ref('int_depression__baseline_features')},
    {'description': 'diagnosis', 'table': ref('int_depression__all_dx_features_grouped')},
    {'description': 'prescribing', 'table': ref('int_depression__all_rx_features_grouped')},
    {'description': 'encounter_counts', 'table': ref('int_depression__all_enctype_features_grouped')},
    {'description': 'insurace', 'table': ref('int_depression__all_insurance_features_grouped')},
    {'description': 'phq_or_epds', 'table': ref('int_depression__phq_or_edinburgh')}
] %}

{% for pair in tables %}
select 
    birthid,
    '{{ pair.description }}' as 'description',
    feature_name,
    value
from {{ pair.table }}
{% if not loop.last %} union all {% endif %}
{% endfor %}

