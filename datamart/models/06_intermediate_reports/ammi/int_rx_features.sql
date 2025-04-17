-- depends_on: {{ ref('daterange') }}

{% set date_range_list = get_date_range('int_rx_features') %}

with cohort as (
    select
        *
    from {{ ref('int_cohort') }}
),

rx as (
    select
        a.patid,
        a.rx_order_date as rx_date,
        a.rxnorm_cui as rx
    from {{ ref('stg_pcornet__prescribing') }} a
    inner join {{ ref('int_selected_rx') }} b on a.rxnorm_cui = b.col
),

renamed as (
    select
        cohort.birthid,
        {{ dbt_utils.pivot('rx',
                            dbt_utils.get_column_values(ref('int_selected_rx'), 'col'),
                            agg='max',
                            then_value=1,
                            else_value=0,
                            prefix='rx_') }}
    from cohort
    left join rx on cohort.mother_patid = rx.patid
     and rx.rx_date between {{ date_range_list[0] }} and {{ date_range_list[1] }}
    group by cohort.birthid
)
select * from renamed