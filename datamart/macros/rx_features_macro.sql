{% macro rx_features_macro(selected_rx_table, date1, date2) %}

with cohort as (
    select
        *
    from {{ ref('int_cohort') }}
),

rx as (
    select
        a.patid,
        {{ add_time_to_date_macro("a.rx_order_date", "a.rx_order_time") }} as rx_date,
        a.rxnorm_cui as rx
    from {{ ref('prescribing') }} a
    inner join {{ ref(selected_rx_table) }} b on a.rxnorm_cui = b.col
),

renamed as (
    select
        cohort.birthid,
        {{ dbt_utils.pivot('rx',
                            dbt_utils.get_column_values(ref(selected_rx_table), column='col', default=[]),
                            agg='max',
                            then_value=1,
                            else_value=0,
                            prefix='rx_') }}
    from cohort
    left join rx on cohort.mother_patid = rx.patid
     and rx.rx_date between {{ date1 }} and {{ date2 }}
    group by cohort.birthid
)
select * from renamed

{% endmacro %}