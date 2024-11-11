SELECT 
    ob_hsb_delivery.summary_block_id        --birth_id
    ,episode_del_rec.ob_del_preg_epi_id     --pregnancy_id
    ,ob_hsb_delivery.ob_del_birth_dttm
    ,ob_hsb_delivery.ob_hx_gest_age
    ,ob_hsb_delivery.ob_hx_outcome_c
    ,ob_hsb_delivery.ob_del_deliv_meth_c
    ,ob_hsb_delivery.baby_birth_csn         --baby birth_csn
    ,ob_hsb_delivery.delivery_date_csn      --mother birth_csn
    ,pat_enc_baby.pat_id                    --baby pat_id
    ,pat_enc_mother.pat_id                  --mother pat_id
FROM 
    ob_hsb_delivery
    JOIN episode episode_del_rec ON ob_hsb_delivery.summary_block_id = episode_del_rec.episode_id
    JOIN episode episode_preg ON episode_del_rec.ob_del_preg_epi_id = episode_preg.episode_id
    JOIN pat_enc pat_enc_baby ON ob_hsb_delivery.baby_birth_csn = pat_enc_baby.pat_enc_csn_id
    JOIN pat_enc pat_enc_mother ON ob_hsb_delivery.delivery_date_csn = pat_enc_mother.pat_enc_csn_id
WHERE
    ob_hsb_delivery.ob_del_epis_type_c = 10
;