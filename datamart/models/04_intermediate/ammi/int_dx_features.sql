-- depends_on: {{ ref('daterange') }}

{% set date_range_list = get_date_range('int_dx_features') %}

with cohort as (
    select
        *
    from {{ ref('int_cohort') }}
),

dx as (
    select
        a.patid,
        a.dx_date,
        a.dx
    from {{ ref('stg_pcornet__diagnosis') }} a
    inner join {{ ref('int_selected_dx') }} b on a.dx = b.col
),

renamed as (
    select
        cohort.birthid,
        {{ dbt_utils.pivot('dx',
                            dbt_utils.get_column_values(ref('int_selected_dx'), 'col'),
                            agg='max',
                            then_value=1,
                            else_value=0,
                            prefix='dx_') }}
    from cohort
    left join dx on cohort.mother_patid = dx.patid
     and dx.dx_date between {{ date_range_list[0] }} and {{ date_range_list[1] }}
    group by cohort.birthid
)
select * from renamed