SELECT 
cmpgn_desc,
cmpgn_in_hm_dt,
count(*)
FROM `cio-datahub-enterprise-pr-183a.ent_cust_intractn.bq_mob_dbm` 
WHERE ccm_ownr IN ('TIAN XIA', 'MARK WU', 'ED LEE', 'ANN LIN', 'JACKIE WANG', 'MICHELLE WU', 'JACOB LI')
group by 1, 2 
ORDER BY 2 DESC 