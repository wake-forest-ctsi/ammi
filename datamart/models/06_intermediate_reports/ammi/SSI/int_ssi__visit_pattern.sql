with visits as (
    select distinct  -- distinct here is necessary to remove duplicated entries 
        birthid,
        enc_type,
        days_since_child_birth
    from {{ ref('int_visits') }}
),

total_counts as (
    select
        birthid,
        sum(case when days_since_child_birth between -268 and -1 then 1 else 0 end) as counts_of_visits_prenatal_care,
        sum(case when days_since_child_birth between 1 and 30 then 1 else 0 end) as counts_of_visits_30d_after_delivery,
        sum(case when days_since_child_birth between 1 and 90 then 1 else 0 end) as counts_of_visits_90d_after_delivery
    from visits
    group by birthid
),

type_counts as (
    select
        a.birthid,
        {% for etype in ['AV', 'IP', 'ED', 'TH', 'OT'] %}
            sum(case when a.enc_type = '{{ etype }}' then 1 else 0 end) as counts_of_{{ etype }}_visits_prenatal_care {% if not loop.last %},{% endif %}
        {% endfor %}
    from (select * from visits where days_since_child_birth between -268 and -1) a
    group by a.birthid
),

renamed as (
    select
        a.birthid,
        a.counts_of_visits_prenatal_care,
        a.counts_of_visits_30d_after_delivery,
        a.counts_of_visits_90d_after_delivery,
        coalesce(b.counts_of_AV_visits_prenatal_care, 0) as counts_of_AV_visits_prenatal_care,
        coalesce(b.counts_of_IP_visits_prenatal_care, 0) as counts_of_IP_visits_prenatal_care,
        coalesce(b.counts_of_ED_visits_prenatal_care, 0) as counts_of_ED_visits_prenatal_care,
        coalesce(b.counts_of_TH_visits_prenatal_care, 0) as counts_of_TH_visits_prenatal_care,
        coalesce(b.counts_of_OT_visits_prenatal_care, 0) as counts_of_OT_visits_prenatal_care
    from total_counts a 
    left join type_counts b on a.birthid = b.birthid
)

select * from renamed