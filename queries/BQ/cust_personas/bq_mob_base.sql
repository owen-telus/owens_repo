SELECT 
  A.bacct_bus_bacct_num AS BAN,
  A.pi_prod_instnc_resrc_str AS MSISDN,
  B.prod_instnc_alias_str AS IMSI
FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` A
INNER JOIN `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_alias_snpsht` B
ON A.bus_prod_instnc_id = B.bus_prod_instnc_id 
AND A.bus_prod_instnc_src_id = B.bus_prod_instnc_src_id
WHERE 
  prod_instnc_ts = TIMESTAMP(CURRENT_DATE() - 1 )  -- Get most recent date in snapshot table
  AND prod_instnc_alias_ts = TIMESTAMP(CURRENT_DATE() - 1)
  AND bacct_brand_id=1 -- 1 For Telus
  AND pi_prod_instnc_typ_cd = 'C'-- Celluluar products 
  AND bacct_bacct_typ_cd = 'I' -- Consumer
  AND bacct_bacct_subtyp_cd = 'R' -- Account Sub type
  AND bacct_bacct_stat_cd = 'O' -- Billing account open  
  AND pi_prod_instnc_stat_cd = 'A' -- Status of product instance
  AND bacct_billg_mthd_cd ='POST' -- Post Pay customers only
  AND prod_instnc_alias_typ_cd='IMSI'
  AND A.bus_prod_instnc_src_id = 130
  AND B.bus_prod_instnc_src_id = 130 