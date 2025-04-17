select 
    birthid,
    {{ dbt_utils.pivot('delivery_mode', 
                       dbt_utils.get_column_values(ref('int_delivery_mode'), 'delivery_mode'),
                       agg='max',
                       then_value=1,
                       else_value=0,
                       prefix='delivery_mode_') }} -- none can also be a pivoted column
from {{ ref('int_delivery_mode') }}
group by birthid