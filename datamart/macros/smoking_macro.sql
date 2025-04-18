{% macro smoking_macro(date1, date2) %}

with cohort as (
    select
        *
    from {{ ref('int_cohort') }}
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
        max(smoking) as smoking,
        max(tobacco) as tobacco
    from cohort
    left join smoking on cohort.mother_patid = smoking.patid
     and measure_date between {{ date1 }} and {{ date2 }}
    group by cohort.birthid
)

select * from renamed

{% endmacro %}