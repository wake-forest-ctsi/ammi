{% set columns = adapter.get_columns_in_relation(ref('stg_censustract__svi_2022_tract')) %}
{% set except_columns = ['st','state','st_abbr','stcnty','county','tractfips','location'] %}

with tract as (
    select
        *
    from {{ ref('stg_censustract__svi_2022_tract')}}
),

zip_tract as (
    select
        *
    from {{ ref('zip_tract') }}
),

renamed as (
    select
        b.zip as zipcode,
        {% for col in columns if col.name not in except_columns %}
            avg({{ col.name }}) as {{ col.name }}_zc {% if not loop.last %},{% endif %}
        {% endfor %}
    from tract a
    inner join zip_tract b on a.tractfips = b.tractfips
    group by b.zip
)

select * from renamed