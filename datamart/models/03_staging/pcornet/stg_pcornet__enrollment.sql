
with source as (

    select * from {{ source('pcornet', 'enrollment') }}

),

renamed as (

    select
        patid,
        enr_start_date,
        enr_end_date,
        chart,
        enr_basis,
        raw_chart,
        raw_basis

    from source

)

select * from renamed

