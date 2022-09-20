SELECT 
  pp_bus_pp_catlg_itm_cd,
 COUNT(*)
FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` 
WHERE 
  prod_instnc_ts = (SELECT MAX(prod_instnc_ts) FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` ) AND -- Get most recent date in snapshot table
  pp_bus_pp_catlg_itm_cd IN ('SVVM8','LSVVM8R','LSVVM8P','LSVM3MR','LSVM3MP','SSPIVVMNE','SVVMNEWR','SIVVMN','SPRVVMN','SPVVMNEW','SNNEWVVM','SPRVVMR','SSVVMR','SVVM3R','SSVISVM3R','SSPVVMR3','SSIVVMR','SVISVM3R','SVVM6MP','SVVMR5') 
GROUP BY pp_bus_pp_catlg_itm_cd
  

SELECT 
PRIM_PRICE_PLAN_CD,
COUNT(*)

FROM `cto-wln-sa-data-pr-bb5283.ref_table.bq_exttable_hpbi_product_instance_profl` 
WHERE PRIM_PRICE_PLAN_CD IN ('SVVM8','LSVVM8R','LSVVM8P','LSVM3MR','LSVM3MP','SSPIVVMNE','SVVMNEWR','SIVVMN','SPRVVMN','SPVVMNEW','SNNEWVVM','SPRVVMR','SSVVMR','SVVM3R','SSVISVM3R','SSPVVMR3','SSIVVMR','SVISVM3R','SVVM6MP','SVVMR5') 

GROUP BY PRIM_PRICE_PLAN_CD 

SELECT 
*
FROM `cto-wln-sa-data-pr-bb5283.ref_table.bq_exttable_hpbi_price_plan_dim` 
WHERE PRICE_PLAN_CD IN ('SVVM8','LSVVM8R','LSVVM8P','LSVM3MR','LSVM3MP','SSPIVVMNE','SVVMNEWR','SIVVMN','SPRVVMN','SPVVMNEW','SNNEWVVM','SPRVVMR','SSVVMR','SVVM3R','SSVISVM3R','SSPVVMR3','SSIVVMR','SVISVM3R','SVVM6MP','SVVMR5') 


SELECT 
soc,
count(DISTINCT billg_acct_num) as count
FROM `cio-datahub-enterprise-pr-183a.ent_cust_bill.bq_wls_chrg_dtl_view` 
WHERE TRIM(UPPER(soc)) IN ('SVVM8','LSVVM8R','LSVVM8P','LSVM3MR','LSVM3MP','SSPIVVMNE','SVVMNEWR','SIVVMN','SPRVVMN','SPVVMNEW','SNNEWVVM','SPRVVMR','SSVVMR','SVVM3R','SSVISVM3R','SSPVVMR3','SSIVVMR','SVISVM3R','SVVM6MP','SVVMR5') 

GROUP BY soc 

SELECT count(DISTINCT(billg_acct_num))
FROM `cio-datahub-enterprise-pr-183a.ent_cust_bill.bq_wls_chrg_dtl_view` 



SELECT 
*
FROM `cio-datahub-enterprise-pr-183a.ent_cust_bill.bq_wls_chrg_dtl_view` A
WHERE --soc IN ('SVVM8','LSVVM8R','LSVVM8P','LSVM3MR','LSVM3MP','SSPIVVMNE','SVVMNEWR','SIVVMN','SPRVVMN','SPVVMNEW','SNNEWVVM','SPRVVMR','SSVVMR','SVVM3R','SSVISVM3R','SSPVVMR3','SSIVVMR','SVISVM3R','SVVM6MP','SVVMR5') 
billg_acct_num = 937438 AND
ent_seq_num = 38538291198
ORDER BY ent_seq_num DESC

SELECT * FROM `cio-datahub-enterprise-pr-183a.ent_cust_bill.bq_wls_bill_dtl_view` 
WHERE billg_acct_num = 937438 
ORDER BY billg_cycl_yr_num DESC, billg_cycl_mth_num DESC


SELECT * FROM `cio-datahub-enterprise-pr-183a.ent_cust_bill.bq_wls_inv_itm_view` 
WHERE bacct=937438
ORDER BY inv_create_dt DESC

SELECT 
A.subscr_num,
B.pp_catlg_itm_nm
FROM `cio-datahub-enterprise-pr-183a.ent_cust_bill.bq_wls_chrg_dtl_view` A
LEFT JOIN (SELECT 
  pi_prod_instnc_resrc_str AS MSISDN,
  pp_catlg_itm_nm 
FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` 
WHERE 
  prod_instnc_ts = (SELECT MAX(prod_instnc_ts) FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` )  -- Get most recent date in snapshot table
) B 
ON A.subscr_num = B.MSISDN 
WHERE A.soc IN ('SVVM8','LSVVM8R','LSVVM8P','LSVM3MR','LSVM3MP','SSPIVVMNE','SVVMNEWR','SIVVMN','SPRVVMN','SPVVMNEW','SNNEWVVM','SPRVVMR','SSVVMR','SVVM3R','SSVISVM3R','SSPVVMR3','SSIVVMR','SVISVM3R','SVVM6MP','SVVMR5') 
AND upper(B.pp_catlg_itm_nm) LIKE '%MAIL%'