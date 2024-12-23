
with source as (

    select * from {{ source('pcornet', 'obs_clin') }}

),

renamed as (

    select
        obsclinid,
        patid,
        encounterid,
        obsclin_providerid,
        obsclin_start_date,
        obsclin_start_time,
        obsclin_stop_date,
        obsclin_stop_time,
        obsclin_type,
        obsclin_code,
        obsclin_result_qual,
        obsclin_result_text,
        obsclin_result_snomed,
        obsclin_result_num,
        obsclin_result_modifier,
        obsclin_result_unit,
        obsclin_source,
        obsclin_abn_ind,
        raw_obsclin_name,
        raw_obsclin_code,
        raw_obsclin_type,
        raw_obsclin_result,
        raw_obsclin_modifier,
        raw_obsclin_unit

    from source

)

select * from renamed

