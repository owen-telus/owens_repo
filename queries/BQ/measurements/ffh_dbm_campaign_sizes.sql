-- Get count of number of customers targeted in each campaign by control and test group + campaign in home date
SELECT 
  cmpgn_cd AS CAMP_ID,
  creative_cd AS CAMP_CREATIVE,
  in_hm_dt,
  ctrl_flg, -- Control Group Flag
  COUNT(*) AS num_customers
FROM `cio-datahub-enterprise-pr-183a.ent_cust_intractn.bq_ffh_dbm` 
GROUP BY 1,2,3,4
ORDER BY 1,2
--WHERE creative_cd	 = 'LWC0222FFHC'