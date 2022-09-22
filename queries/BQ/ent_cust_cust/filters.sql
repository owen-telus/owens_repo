-- SELECT 
--   COUNT(*)
-- FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` 
-- WHERE 
--   prod_instnc_ts = (SELECT MAX(prod_instnc_ts) FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` )  -- Get most recent date in snapshot table
--   AND bacct_brand_id=1 
--   AND 

SELECT 
  bacct_bacct_stat_cd,
  COUNT(*)
 FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` 
WHERE 
  prod_instnc_ts = (SELECT MAX(prod_instnc_ts) FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` )  -- Get most recent date in snapshot table
GROUP BY bacct_bacct_stat_cd

SELECT 
  COUNT(*)
FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` 
WHERE 
  prod_instnc_ts = (SELECT MAX(prod_instnc_ts) FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` )  -- Get most recent date in snapshot table
  AND bacct_brand_id=1 -- 1 For Telus
  AND pi_prod_instnc_typ_cd = 'C'-- Celluluar products 
  AND bacct_bacct_typ_cd = 'I' -- Consumer
  AND bacct_bacct_subtyp_cd = 'R' -- Account Sub type
  AND bacct_bacct_stat_cd = 'O' -- Billing account open  
  AND pi_prod_instnc_stat_cd = 'A' -- Status of product instance
  AND bacct_billg_mthd_cd ='POST' -- Post Pay customers only
  AND (UPPER(pp_catlg_itm_nm) LIKE '%TABLET%' OR UPPER(pp_catlg_itm_nm) LIKE '%WATCH%')


SELECT 
  COUNT(*),
  pi_prod_instnc_stat_cd
FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` 
WHERE 
  prod_instnc_ts = (SELECT MAX(prod_instnc_ts) FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` )  -- Get most recent date in snapshot table

GROUP BY pi_prod_instnc_stat_cd

SELECT 
  COUNT(*),
  bacct_billg_mthd_cd
FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` 
WHERE 
  prod_instnc_ts = (SELECT MAX(prod_instnc_ts) FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` )  -- Get most recent date in snapshot table

GROUP BY bacct_billg_mthd_cd

SELECT 
  COUNT(*),
  bacct_kb_pymt_mthd_cd
FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` 
WHERE 
  prod_instnc_ts = (SELECT MAX(prod_instnc_ts) FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` )  -- Get most recent date in snapshot table

GROUP BY bacct_kb_pymt_mthd_cd


SELECT 
  COUNT(*),
  bacct_kb_billg_cycl_cd
FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` 
WHERE 
  prod_instnc_ts = (SELECT MAX(prod_instnc_ts) FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` )  -- Get most recent date in snapshot table

GROUP BY bacct_kb_billg_cycl_cd

SELECT
  pp_recur_chrg_amt,
  pi_prod_instnc_resrc_str
FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` 
WHERE 
  prod_instnc_ts = (SELECT MAX(prod_instnc_ts) FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` )  -- Get most recent date in snapshot table
  AND pi_prod_instnc_resrc_str='7786864841'

SELECT

  BACCT_BUS_BACCT_NUM,
  pi_prod_instnc_resrc_str,
  pp_recur_chrg_amt,
  pp_bus_pp_catlg_itm_cd
FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` 
WHERE 
  prod_instnc_ts = (SELECT MAX(prod_instnc_ts) FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` )  -- Get most recent date in snapshot table
  AND bacct_brand_id=1 
  AND pi_prod_instnc_typ_cd = 'C'
  AND bacct_bacct_typ_cd = 'I'
  AND bacct_bacct_subtyp_cd = 'R' 
  AND bacct_bacct_stat_cd = 'O'
ORDER BY pp_bus_pp_catlg_itm_cd DESC 

SELECT
  COUNT(*) as duplicates,
  BACCT_BUS_BACCT_NUM,
  pi_prod_instnc_resrc_str
FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` 
WHERE 
  prod_instnc_ts = (SELECT MAX(prod_instnc_ts) FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` )  -- Get most recent date in snapshot table
  AND bacct_brand_id=1 
  AND pi_prod_instnc_typ_cd = 'C'
  AND bacct_bacct_typ_cd = 'I'
  AND bacct_bacct_subtyp_cd = 'R' 
  AND bacct_bacct_stat_cd = 'O'
GROUP BY BACCT_BUS_BACCT_NUM,pi_prod_instnc_resrc_str

ORDER BY duplicates DESC 

SELECT 
  *
FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` 
WHERE 
  prod_instnc_ts = (SELECT MAX(prod_instnc_ts) FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` ) 
  AND pi_prod_instnc_resrc_str='4035409765'


SELECT 
  pp_bus_pp_catlg_itm_cd AS soc_cd,
  MAX(pp_recur_chrg_amt) AS price_plan_amt,
  MAX(pp_catlg_itm_nm) AS price_plan_txt
FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` 
WHERE 
  prod_instnc_ts = (SELECT MAX(prod_instnc_ts) FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` ) 
GROUP BY pp_bus_pp_catlg_itm_cd
ORDER BY price_plan_amt DESC


-- SELECT 
--   COUNT(*)
-- FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` 
-- WHERE 
--   prod_instnc_ts = (SELECT MAX(prod_instnc_ts) FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` )  -- Get most recent date in snapshot table
--   AND bacct_brand_id=1 
--   AND 

SELECT 
  bacct_bacct_stat_cd,
  COUNT(*)
 FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` 
WHERE 
  prod_instnc_ts = (SELECT MAX(prod_instnc_ts) FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` )  -- Get most recent date in snapshot table
GROUP BY bacct_bacct_stat_cd

SELECT 
  COUNT(*)
FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` 
WHERE 
  prod_instnc_ts = (SELECT MAX(prod_instnc_ts) FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` )  -- Get most recent date in snapshot table
  AND bacct_brand_id=1 -- 1 For Telus
  AND pi_prod_instnc_typ_cd = 'C'-- Celluluar products 
  AND bacct_bacct_typ_cd = 'I' -- Consumer
  AND bacct_bacct_subtyp_cd = 'R' -- Account Sub type
  AND bacct_bacct_stat_cd = 'O' -- Billing account open  
  AND pi_prod_instnc_stat_cd = 'A' -- Status of product instance
  AND bacct_billg_mthd_cd ='POST' -- Post Pay customers only
  AND (UPPER(pp_catlg_itm_nm) LIKE '%TABLET%' OR UPPER(pp_catlg_itm_nm) LIKE '%WATCH%')


SELECT 
  COUNT(*),
  pi_prod_instnc_stat_cd
FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` 
WHERE 
  prod_instnc_ts = (SELECT MAX(prod_instnc_ts) FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` )  -- Get most recent date in snapshot table

GROUP BY pi_prod_instnc_stat_cd

SELECT 
  COUNT(*),
  bacct_billg_mthd_cd
FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` 
WHERE 
  prod_instnc_ts = (SELECT MAX(prod_instnc_ts) FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` )  -- Get most recent date in snapshot table

GROUP BY bacct_billg_mthd_cd

SELECT 
  COUNT(*),
  bacct_kb_pymt_mthd_cd
FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` 
WHERE 
  prod_instnc_ts = (SELECT MAX(prod_instnc_ts) FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` )  -- Get most recent date in snapshot table

GROUP BY bacct_kb_pymt_mthd_cd


SELECT 
  COUNT(*),
  bacct_kb_billg_cycl_cd
FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` 
WHERE 
  prod_instnc_ts = (SELECT MAX(prod_instnc_ts) FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` )  -- Get most recent date in snapshot table

GROUP BY bacct_kb_billg_cycl_cd

SELECT
  pp_recur_chrg_amt,
  pi_prod_instnc_resrc_str
FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` 
WHERE 
  prod_instnc_ts = (SELECT MAX(prod_instnc_ts) FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` )  -- Get most recent date in snapshot table
  AND pi_prod_instnc_resrc_str='7786864841'

SELECT

  BACCT_BUS_BACCT_NUM,
  pi_prod_instnc_resrc_str,
  pp_recur_chrg_amt,
  pp_bus_pp_catlg_itm_cd
FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` 
WHERE 
  prod_instnc_ts = (SELECT MAX(prod_instnc_ts) FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` )  -- Get most recent date in snapshot table
  AND bacct_brand_id=1 
  AND pi_prod_instnc_typ_cd = 'C'
  AND bacct_bacct_typ_cd = 'I'
  AND bacct_bacct_subtyp_cd = 'R' 
  AND bacct_bacct_stat_cd = 'O'
ORDER BY pp_bus_pp_catlg_itm_cd DESC 

SELECT
  COUNT(*) as duplicates,
  BACCT_BUS_BACCT_NUM,
  pi_prod_instnc_resrc_str
FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` 
WHERE 
  prod_instnc_ts = (SELECT MAX(prod_instnc_ts) FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` )  -- Get most recent date in snapshot table
  AND bacct_brand_id=1 
  AND pi_prod_instnc_typ_cd = 'C'
  AND bacct_bacct_typ_cd = 'I'
  AND bacct_bacct_subtyp_cd = 'R' 
  AND bacct_bacct_stat_cd = 'O'
GROUP BY BACCT_BUS_BACCT_NUM,pi_prod_instnc_resrc_str

ORDER BY duplicates DESC 

SELECT 
  *
FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` 
WHERE 
  prod_instnc_ts = (SELECT MAX(prod_instnc_ts) FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` ) 
  AND pi_prod_instnc_resrc_str='4035409765'


SELECT 
  pp_bus_pp_catlg_itm_cd AS soc_cd,
  MAX(pp_recur_chrg_amt) AS price_plan_amt,
  MAX(pp_catlg_itm_nm) AS price_plan_txt
FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` 
WHERE 
  prod_instnc_ts = (SELECT MAX(prod_instnc_ts) FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` ) 
GROUP BY pp_bus_pp_catlg_itm_cd
ORDER BY price_plan_amt DESC