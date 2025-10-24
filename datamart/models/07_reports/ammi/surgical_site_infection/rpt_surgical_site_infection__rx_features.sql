select
    birthid,
    rxnorm_cui,
    max(datediff(day, rx_order_date, baby_birth_date)) as earliest_day,
    min(datediff(day, rx_order_date, baby_birth_date)) as latest_day,
    1 as has_rxnorm_cui
from {{ ref('int_ssi__all_rx_features') }}
group by birthid, rxnorm_cui