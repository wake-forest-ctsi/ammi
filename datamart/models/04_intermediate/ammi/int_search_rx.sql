-- depends_on: {{ ref('daterange') }}

{% set date_range_list = get_date_range('int_search_dx') %}

with cohort as (
    select
        *
    from {{ ref('int_cohort') }}
),

rx as (
    select
        patid,
        rxnorm_cui,
        rx_order_date
    from {{ ref('stg_pcornet__prescribing') }}
),

rx_tmp as (
    select
        cohort.birthid,
        rxnorm_cui as rx,
        min(rx.rx_order_date) as earliest_date
    from cohort
    left join rx on cohort.mother_patid = rx.patid
     and rx.rx_order_date between {{ date_range_list[0] }} and {{ date_range_list[1] }}
    group by cohort.birthid, rxnorm_cui
),

renamed as (
    select
        *,
        count(birthid) over (partition by rx) as pat_counts
    from rx_tmp
)

select * from renamed where pat_counts >= 50