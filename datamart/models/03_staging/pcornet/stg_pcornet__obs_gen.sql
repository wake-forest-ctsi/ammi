
with source as (

    select * from {{ source('pcornet', 'obs_gen') }}

),

renamed as (

    select
        obsgenid,
        patid,
        encounterid,
        obsgen_providerid,
        obsgen_start_date,
        obsgen_start_time,
        obsgen_stop_date,
        obsgen_stop_time,
        obsgen_type,
        obsgen_code,
        obsgen_result_qual,
        obsgen_result_text,
        obsgen_result_num,
        obsgen_result_modifier,
        obsgen_result_unit,
        obsgen_table_modified,
        obsgen_id_modified,
        obsgen_source,
        obsgen_abn_ind,
        raw_obsgen_name,
        raw_obsgen_code,
        raw_obsgen_type,
        raw_obsgen_result,
        raw_obsgen_unit

    from source

)

select * from renamed

