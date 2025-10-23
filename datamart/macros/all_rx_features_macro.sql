{% macro all_rx_features_macro(min_count, date1, date2, cohort_filter='') %}

-- by default truncating the dx to 5 char, except keepting all chars for O, T, Z codes
-- it only outputs dx observed with patients > min_count

with cohort as (
    select
        {{ dbt_utils.star(from=ref('int_cohort'), relation_alias='cohort') }}
    from {{ ref('int_cohort') }} cohort
    {% if cohort_filter | length > 0 %}
    {{ cohort_filter }}
    {% endif %}
),

rx as (
    select
        patid,
        rxnorm_cui,
        {{ add_time_to_date_macro('rx_order_date', 'rx_order_time') }} as rx_order_date,
        lower(raw_rx_med_name) as rx_name 
    from {{ ref('prescribing') }}
),

rx_selected as (
    select
        cohort.birthid,
        cohort.mother_patid,
        cohort.baby_birth_date,
        rx.rxnorm_cui,
        rx.rx_order_date,
        rx_name
    from cohort
    left join rx on cohort.mother_patid = rx.patid 
     and rx.rx_order_date between {{ date1 }} and {{ date2 }}
),

rx_count as (
    select
        rxnorm_cui
    from rx_selected
    group by rxnorm_cui
    having count(distinct birthid) >= {{ min_count }}
),

renamed as (
    select
        rx_selected.birthid,
        rx_selected.mother_patid,
        rx_selected.baby_birth_date,
        rx_selected.rxnorm_cui,
        rx_selected.rx_order_date,
        rx_selected.rx_name
    from rx_selected
    inner join rx_count on rx_selected.rxnorm_cui = rx_count.rxnorm_cui
)

select * from renamed

{% endmacro %}