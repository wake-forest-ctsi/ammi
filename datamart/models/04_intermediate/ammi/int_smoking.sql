-- depends_on: {{ ref('daterange') }}

{% set date_range_list = get_date_range('int_smoking') %}

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
        measure_date
    from {{ ref('stg_pcornet__vital') }}
),

renamed as (
    select
        cohort.birthid,
        max(smoking) as smoking,
        max(tobacco) as tobacco
    from cohort
    left join smoking on cohort.mother_patid = smoking.patid
     and measure_date between {{ date_range_list[0] }} and {{ date_range_list[1] }}
    group by cohort.birthid
)

select * from renamed