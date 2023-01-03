-- unique subscribers targeted for marketing campaign
SELECT 
  EXTRACT(YEAR FROM src_create_dt) AS year_,
  EXTRACT(MONTH FROM src_create_dt) AS month_,
  COUNT(DISTINCT subscr_num) AS unique_msisdn
FROM `cio-datahub-enterprise-pr-183a.ent_cust_intractn.bq_mob_dbm` 
GROUP BY 1,2
ORDER BY 1,2
