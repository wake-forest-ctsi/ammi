select 
    birthid,
    {{ dbt_utils.pivot('enc_type', 
                       dbt_utils.get_column_values(ref('int_delivery_enc_type'), 'enc_type', default=[]),
                       agg='max',
                       then_value=1,
                       else_value=0,
                       prefix='delivery_enc_type_') }}, -- none can also be a pivoted column
    {{ dbt_utils.pivot('admitting_source', 
                        dbt_utils.get_column_values(ref('int_delivery_enc_type'), 'admitting_source', default=[]),
                        agg='max',
                        then_value=1,
                        else_value=0,
                        prefix='delivery_admitting_source_') }} -- none can also be a pivoted column
from {{ ref('int_delivery_enc_type') }}
group by birthid