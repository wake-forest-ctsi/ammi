
{% if var("report") == 'depression' %}
    with tmp as (
        select
            col as col,
            shap as shap,
            row_number() over (order by shap) as k
        from {{ ref('useful_rx_depression') }}
    )
    select col, shap from tmp where k <= 100

{% else %}
    select
        distinct rxnorm_cui
    from {{ ref('stg_pcornet__prescribing') }}

{% endif %}