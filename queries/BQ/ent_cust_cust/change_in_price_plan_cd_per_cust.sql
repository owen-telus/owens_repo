SELECT
  *
FROM (
  SELECT 
    
    BACCT_BUS_BACCT_NUM AS BAN,
    pi_prod_instnc_resrc_str AS MSISDN,
    prod_instnc_ts,
    pp_bus_pp_catlg_itm_cd,
    pp_catlg_itm_nm,
    pp_recur_chrg_amt,
    ROW_NUMBER() OVER(PARTITION BY BACCT_BUS_BACCT_NUM, pi_prod_instnc_resrc_str, pp_bus_pp_catlg_itm_cd ORDER BY prod_instnc_ts ASC ) AS ROW_NUM
  FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` 
  WHERE 
    DATE(prod_instnc_ts) <> "2022-07-29" 
    AND pi_prod_instnc_typ_cd = 'C'-- Celluluar products 
    AND bus_prod_instnc_src_id = 130 

)
WHERE 
  ROW_NUM=1 
  AND MSISDN='7788635688'
ORDER BY MSISDN