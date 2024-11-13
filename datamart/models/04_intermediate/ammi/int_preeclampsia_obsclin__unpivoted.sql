{{ config(materialized='view', ) }}



with unpivoted as (
    {{
            dbt_utils.unpivot(
                relation=ref("int_preeclampsia_obsclin__aggregated")
                ,cast_to="float"
                ,exclude=["birth_id","obsclin_type","obsclin_code"]
                ,field_name="aggregation"
                ,value_name="value"
            )
    }}
)

select
    birth_id
    , concat(obsclin_type,'_',obsclin_code,'_',aggregation) as grouping
    , value
from 
    unpivoted