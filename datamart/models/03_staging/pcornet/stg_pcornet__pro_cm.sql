
with source as (

    select * from {{ source('pcornet', 'pro_cm') }}

),

renamed as (

    select
        pro_cm_id,
        patid,
        encounterid,
        pro_date,
        pro_time,
        pro_type,
        pro_item_name,
        pro_item_loinc,
        pro_response_text,
        pro_response_num,
        pro_method,
        pro_mode,
        pro_cat,
        pro_source,
        pro_item_version,
        pro_measure_name,
        pro_measure_seq,
        pro_measure_score,
        pro_measure_theta,
        pro_measure_scaled_tscore,
        pro_measure_standard_error,
        pro_measure_count_scored,
        pro_measure_loinc,
        pro_measure_version,
        pro_item_fullname,
        pro_item_text,
        pro_measure_fullname

    from source

)

select * from renamed

