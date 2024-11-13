{{ config(materialized='view', ) }}


select
    birth_id,
    {{ dbt_utils.pivot(    
        column='grouping'
        ,values=dbt_utils.get_column_values(ref('int_preeclampsia_obsclin__unpivoted'), 'grouping')
        ,agg='sum'
        ,then_value='value'
        ,else_value='NULL'
    ) }}
from {{ ref('int_preeclampsia_obsclin__unpivoted') }}
group by birth_id