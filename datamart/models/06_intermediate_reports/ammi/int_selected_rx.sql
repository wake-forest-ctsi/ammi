
{% if var("report") == 'depression' %}
    with tmp as (
        select
            feature as col,
            shap_edinburgh,
            shap_phq9,
            shap_F53,
            row_number() over (order by shap_edinburgh desc) as k1,
            row_number() over (order by shap_phq9 desc) as k2,
            row_number() over (order by shap_F53 desc) as k3
        from {{ ref('useful_rx_depression') }}
    )
    select
        col,
        shap_edinburgh,
        shap_phq9,
        shap_F53
    from tmp
    where k1 <= 100 or k2 <= 100 or k3 <= 100

{% else %}
    select
        distinct rxnorm_cui
    from {{ ref('stg_pcornet__prescribing') }}

{% endif %}