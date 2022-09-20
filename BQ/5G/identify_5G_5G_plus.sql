WITH table_ as 
(
  SELECT 
  pi_prod_instnc_resrc_str AS MSISDN,
  pp_recur_chrg_amt,
  pi_prim_pp_sls_start_ts,
  pp_catlg_itm_nm, 
  CASE 
        WHEN pp_catlg_itm_nm LIKE '%5G+%' THEN '5G+'
        WHEN pp_catlg_itm_nm LIKE '%5G %' THEN '5G'
        ELSE 'False'
  END AS plan_5G_5G_plus

FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` 
WHERE 
  prod_instnc_ts = (SELECT MAX(prod_instnc_ts) FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` )  -- Get most recent date in snapshot table

)

SELECT * 
FROM table_
WHERE
  plan_5G_5G_plus = '5G+'
ORDER BY pp_recur_chrg_amt DESC