{% macro dx_features_macro(selected_dx_table, date1, date2) %}

with cohort as (
    select
        *
    from {{ ref('int_cohort') }}
),

dx as (
    select
        a.patid,
        a.dx_date,   -- diagnosis only has date
        a.dx
    from {{ ref('diagnosis') }} a
    inner join {{ ref(selected_dx_table) }} b on a.dx = b.col
),

renamed as (
    select
        cohort.birthid,
        {{ dbt_utils.pivot('dx',
                            dbt_utils.get_column_values(ref(selected_dx_table), 'col'),
                            agg='max',
                            then_value=1,
                            else_value=0,
                            prefix='dx_') }}
    from cohort
    left join dx on cohort.mother_patid = dx.patid
     and dx.dx_date between {{ date1 }} and {{ date2 }}
    group by cohort.birthid
)

select * from renamed

{% endmacro %}