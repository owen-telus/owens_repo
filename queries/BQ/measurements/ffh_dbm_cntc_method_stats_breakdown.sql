-- SELECT 
--   EXTRACT(YEAR FROM in_hm_dt) AS year,
--   EXTRACT(QUARTER FROM in_hm_dt) AS quarter,
--   cntct_med_desc,
--   MIN(in_hm_dt) AS in_hm_dt,
--   COUNT(*) AS num_records
-- FROM `cio-datahub-enterprise-pr-183a.ent_cust_intractn.bq_ffh_dbm` 
-- --WHERE in_hm_dt >= '2022-01-01'
-- GROUP BY 1,2,3
-- ORDER BY 1 DESC, 2 DESC, 3 DESC

SELECT 
  EXTRACT(YEAR FROM in_hm_dt) AS year,
  EXTRACT(QUARTER FROM in_hm_dt) AS quarter,
  cmpgn_typ_desc,
  MIN(in_hm_dt) AS in_hm_dt,
  COUNT(*) AS num_records
FROM `cio-datahub-enterprise-pr-183a.ent_cust_intractn.bq_ffh_dbm` 
WHERE DATE(in_hm_dt) BETWEEN DATE('2015-01-01') AND CURRENT_DATE()
GROUP BY 1,2,3
ORDER BY 1 DESC, 2 DESC, 3 DESC
