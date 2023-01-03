SELECT  
  ban_id,
  subscr_num,
  CONCAT(EXTRACT(YEAR FROM cmpgn_in_hm_dt), '_', EXTRACT(QUARTER FROM cmpgn_in_hm_dt) ) AS year_quarter,
  COUNT(*) AS num_records
FROM `cio-datahub-enterprise-pr-183a.ent_cust_intractn.bq_mob_dbm` 
WHERE cmpgn_in_hm_dt IS NOT NULL 
GROUP BY 1,2,3 
ORDER BY 4 DESC 
