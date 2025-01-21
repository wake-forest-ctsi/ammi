-- convert payer_type_primary to pram numbers

select
    patid,
    admit_date,
    case 
        when payer_type_primary like '2%' then 1
        when payer_type_primary like '5%' then 2
        when payer_type_primary = '81' then 3
        when payer_type_primary = '311' then 5
        when payer_type_primary like '1%' or payer_type_primary like '3%' then 6
        when payer_type_primary like '9%' then 8
        else 8 end as pay
from {{ ref('stg_pcornet__encounter') }}
where payer_type_primary != 'NI'