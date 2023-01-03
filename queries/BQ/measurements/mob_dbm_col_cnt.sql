SELECT  
EXTRACT(YEAR FROM cmpgn_in_hm_dt) AS year,
EXTRACT(QUARTER FROM cmpgn_in_hm_dt) AS quarter,
cmpgn_typ_desc,
MIN(cmpgn_in_hm_dt) AS cmpgn_in_hm_dt,
COUNT(*) AS num_records
FROM `cio-datahub-enterprise-pr-183a.ent_cust_intractn.bq_mob_dbm` 
WHERE cmpgn_in_hm_dt IS NOT NULL 
GROUP BY 1,2,3


