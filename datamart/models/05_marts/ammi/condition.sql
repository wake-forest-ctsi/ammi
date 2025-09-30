select
    conditionid,
    patid,
    encounterid,
    cast(report_date as date) report_date,
    cast(resolve_date as date) resolve_date,
    cast(onset_date as date) onset_date,
    condition_status,
    condition,
    condition_type,
    condition_source,
    raw_condition_status,
    raw_condition,
    raw_condition_type,
    raw_condition_source
from
    {{ ref('stg_pcornet__condition') }}