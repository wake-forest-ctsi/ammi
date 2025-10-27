{% macro all_smoking_macro(cohort_table, date1, date2) %}

with cohort as (
    select
        *
    from {{ cohort_table }}
),

smoking as (
    select
        patid,
        case when smoking in ('01', '02', '05', '07', '08') then 1 else 0 end as 'smoking',
        case when tobacco in ('01') then 1 else 0 end as 'tobacco',
        {{ add_time_to_date_macro("measure_date", "measure_time") }} as measure_date
    from {{ ref('vital') }}
),

renamed as (
    select
        cohort.birthid,
        smoking,
        tobacco
    from cohort
    left join smoking on cohort.mother_patid = smoking.patid
     and measure_date between {{ date1 }} and {{ date2 }}
)

select * from renamed

{% endmacro %}