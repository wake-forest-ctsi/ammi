{% set sdoh_columns = (
    'acs_median_hh_inc',
    'acs_pct_inc50',
    'acs_pct_person_inc_100_124',
    'acs_pct_person_inc_125_199',
    'acs_pct_person_inc_above200',
    'acs_pct_person_inc_below99',
    'acs_pct_college_associate_dgr',
    'acs_pct_bachelor_dgr',
    'acs_pct_no_work_no_schl_16_19',
    'acs_pct_graduate_dgr',
    'acs_pct_hs_graduate',
    'acs_pct_lt_hs',
    'acs_pct_posths_ed') %}
    
{% set svi_columns = (
    'epl_pov150',
    'epl_unemp',
    'epl_hburd',
    'epl_nohsdp',
    'epl_uninsur',
    'rpl_theme1',
    'rpl_theme2',
    'rpl_theme3',
    'rpl_theme4',
    'rpl_themes'
) %}

with cohort as (
    select
        birthid,
        mother_patid,
        baby_birth_date
    from {{ ref('int_cohort') }}
),

address as (
    select
        addressid,
        address_zip5 as zipcode,
        patid as mother_patid,
        address_period_start,
        address_period_end
    from {{ ref('stg_pcornet__private_address_history') }}
),

geocode as (
    select
        addressid,
        min(geocode_custom) as tractfips,
        min(geocode_longitude) as longitude,
        min(geocode_latitude) as latitude
    from {{ ref('stg_pcornet__private_address_geocode') }}
    group by addressid  -- there're some repeating addressid in the base
),

address_selected_tmp1 as (
    select
        a.birthid,
        b.addressid,
        zipcode,
        case when baby_birth_date between address_period_start and address_period_end then 0
             else least(abs(datediff(day, baby_birth_date, address_period_start)),
                        abs(datediff(day, address_period_end, baby_birth_date))) end as closest_day
    from cohort a
    left join address b on a.mother_patid = b.mother_patid
     and baby_birth_date between dateadd(year, -2, address_period_start) and dateadd(year, 2, address_period_end)
),

address_selected_tmp2 as (
    select
        birthid,
        addressid,
        zipcode,
        closest_day,
        row_number() over (partition by birthid order by closest_day) as k
    from address_selected_tmp1
    where closest_day < 365*2 or closest_day is null -- keep the null values
),

address_selected as (
    select
        birthid,
        addressid,
        zipcode,
        closest_day,
        (case when closest_day is null then 0 else 1 end) as has_address
    from address_selected_tmp2
    where k = 1
),

address_selected_geocoded as (
    select
        a.birthid,
        a.addressid,
        a.zipcode,
        a.closest_day,
        a.has_address,
        b.tractfips,
        (case when b.tractfips is null then 0 else 1 end) as has_tractfips,
        b.longitude,
        b.latitude
    from address_selected a
    left join geocode b on a.addressid = b.addressid
),

renamed as (
    select
        a.*,
        {% for col in sdoh_columns %}
            coalesce(b.{{ col }}, c.{{ col }}_zc) as '{{ col }}',
        {% endfor %}
        {% for col in svi_columns  %}
            coalesce(d.{{ col }}, e.{{ col }}_zc) as '{{ col }}' {% if not loop.last %},{% endif %}
        {% endfor %}
    from address_selected_geocoded a
    left join {{ ref('stg_censustract__sdoh_2020_tract') }} b on a.tractfips = b.tractfips
    left join {{ ref('stg_censustract__sdoh_2020_zipcode')}} c on a.zipcode = c.zipcode
    left join {{ ref('stg_censustract__svi_2022_tract') }} d on a.tractfips = d.tractfips
    left join {{ ref('int_censustract__svi_2022_zipcode') }} e on a.zipcode = e.zipcode
)

select * from renamed;

