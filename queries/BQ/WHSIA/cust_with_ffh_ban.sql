-- GET WHSIA Customers who have FFH BAN and see if they have a mobility account
WITH FFH_CUST AS (
SELECT 
  prod_instnc_ts, 
  bus_prod_instnc_id, 
  pi_prod_instnc_resrc_str AS MSISDN,
  bus_prod_instnc_src_id, 
  BACCT_BUS_BACCT_NUM AS FFH_BAN,  
  pp_bus_pp_catlg_itm_cd , 
  pp_catlg_itm_nm
FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` 
WHERE 
  prod_instnc_ts = (SELECT MAX(prod_instnc_ts) FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` ) AND -- Get most recent date in snapshot table
  pp_bus_pp_catlg_itm_cd IN (SELECT whsia_soc FROM `cto-wln-sa-data-pr-bb5283.ref_table.bq_whsia_soc_codes`) AND --Get WHSIA SOC Codes
  bus_prod_instnc_src_id=1001 -- GET FFH BAN


)

SELECT
  A.*,
  B.MOB_BAN,
  CASE WHEN B.MOB_BAN IS NULL THEN 'N' ELSE 'Y' END AS HAS_MOB_BAN --By doing a left join with mnh_ban_mapping, if MOB_BAN is NULL, then FFH Cust doesn't have MOB BAN, if it contains a MOB_BAN, then that is their MOB_BAN 
FROM FFH_CUST A 
LEFT JOIN  `cto-wln-sa-data-pr-bb5283.ref_table.bq_hpbi_mnh_ban_mapping` B
ON A.FFH_BAN = B.FFH_BAN 
ORDER BY HAS_MOB_BAN DESC
