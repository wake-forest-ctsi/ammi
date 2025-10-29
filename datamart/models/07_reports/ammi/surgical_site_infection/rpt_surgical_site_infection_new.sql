{% set tables = [
    {'description': 'phenotype', 'table': ref('int_ssi__phenotype')},
    {'description': 'baseline', 'table': ref('int_ssi__baseline_features')},
    {'description': 'diagnosis', 'table': ref('int_ssi__all_dx_features_grouped')},
    {'description': 'prescribing', 'table': ref('int_ssi__all_rx_features_grouped')},
    {'description': 'encounter_counts', 'table': ref('int_ssi__all_enctype_features_grouped')},
    {'description': 'insurace', 'table': ref('int_ssi__all_insurance_features_grouped')},
    {'description': 'lab_numerical', 'table': ref('int_ssi__all_lab_numerical_features_grouped')},
    {'description': 'lab_text', 'table': ref('int_ssi__all_lab_text_features_grouped')},
    {'description': 'obsclin_numerical', 'table': ref('int_ssi__all_obsclin_numerical_features_grouped')},
    {'description': 'obsclin_text', 'table': ref('int_ssi__all_obsclin_text_features_grouped')}
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

