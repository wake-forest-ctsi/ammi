-- get more details counts for the following dx codes
{% set values = ('F32', 'O99', 'F41', 'F33') %}

with tmp as (
    select
        birthid,
        dx,
        datediff(day, dx_date, baby_birth_date) as dx_date
    from {{ ref('int_mental_disorder') }}
),

renamed as (
    select
        birthid,
        {% for value in values %}
            sum(case when dx = '{{ value }}' and dx_date = 0 then 1 else 0 end) 
                 as {{ value }}_same_day_count,
            sum(case when dx = '{{ value }}' and dx_date between 1 and 30 then 1 else 0 end) 
                 as {{ value }}_one_month_count,
            sum(case when dx = '{{ value }}' and dx_date between 31 and 90 then 1 else 0 end) 
                 as {{ value }}_three_month_count,
            sum(case when dx = '{{ value }}' and dx_date between 91 and 180 then 1 else 0 end) 
                 as {{ value }}_six_month_count,
            sum(case when dx = '{{ value }}' and dx_date between 181 and 270 then 1 else 0 end) 
                 as {{ value }}_nine_month_count,
            sum(case when dx = '{{ value }}' and dx_date between 271 and 365 then 1 else 0 end) 
                 as {{ value }}_one_year_count {% if not loop.last%},{% endif %}
        {% endfor %}
    from tmp
    group by birthid
)

select * from renamed