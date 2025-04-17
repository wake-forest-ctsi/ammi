select
    medadminid,
    patid,
    encounterid,
    prescribingid,
    medadmin_providerid,
    cast(medadmin_start_date as date) medadmin_start_date,
    medadmin_start_time,
    cast(medadmin_stop_date as date) medadmin_stop_date,
    medadmin_stop_time,
    medadmin_type,
    medadmin_code,
    medadmin_dose_admin,
    medadmin_dose_admin_unit,
    medadmin_route,
    medadmin_source,
    raw_medadmin_med_name,
    raw_medadmin_code,
    raw_medadmin_dose_admin,
    raw_medadmin_dose_admin_unit,
    raw_medadmin_route
from
    {{ source('pcornet', 'med_admin') }}