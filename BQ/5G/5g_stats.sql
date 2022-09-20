-- SELECT 
--   SUM(CASE
--     WHEN pp_catlg_itm_nm LIKE '%5G+%' THEN 1
--     WHEN pp_catlg_itm_nm LIKE '%5G %' THEN 1
--     ELSE 0
--   END) / COUNT(*) AS perc_5G
-- FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` 
-- WHERE 
--   prod_instnc_ts = (SELECT MAX(prod_instnc_ts) FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` )  -- Get most recent date in snapshot table
--   AND bacct_brand_id=1 -- 1 For Telus
--   AND pi_prod_instnc_typ_cd = 'C'-- Celluluar products 
--   AND bacct_bacct_typ_cd = 'I' -- Consumer
--   AND bacct_bacct_subtyp_cd = 'R' -- Account Sub type
--   AND bacct_bacct_stat_cd = 'O' -- Billing account open  
--   AND pi_prod_instnc_stat_cd = 'A' -- Status of product instance
--   AND bacct_billg_mthd_cd ='POST' -- Post Pay customers only
--   --AND (UPPER(pp_catlg_itm_nm) NOT LIKE '%TABLET%' OR UPPER(pp_catlg_itm_nm) NOT LIKE '%WATCH%')


SELECT 

  SUM(CASE WHEN is_5g_capable = 'Yes' THEN 1 ELSE 0 END) / COUNT(*) AS perc_5g_capable_device
FROM `cto-wln-sa-data-pr-bb5283.ref_table.bq_hpbi_product_instance_profl` A
LEFT JOIN `cto-wln-sa-data-pr-bb5283.customer_personas_features.tac_info` B 
ON A.TAC_ID= B.tac_id
WHERE
  BRAND_CD = '1' -- 1 = Telus, 3 = Koodo, 7 = Public Mobile, 6 = PC Mobile
  AND CUST_TYPE_CD = 'R' -- CUST_TYPE_TXT: R = Customer type individual / Not Available, B = Business / Not Available
  AND PROD_INSTNC_TYPE_TXT='Cellular' 
  AND PROD_INSTNC_STAT_CD='A' -- PROD_INSTNC_STAT_TXT: A = Active, S = Suspended, C = Deactive, P = Pre-Activated

