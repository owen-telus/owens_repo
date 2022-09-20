SELECT 

PROD_INSTNC_MASTER_SRC_ID,
PROD_INSTNC_ALIAS_TYPE_CD,
PROD_INSTNC_STAT_TXT,
PROD_INSTNC_TYPE_TXT,
COUNT(*)
FROM `cto-wln-sa-data-pr-bb5283.ref_table.bq_hpbi_product_instance_profl` 
GROUP BY PROD_INSTNC_MASTER_SRC_ID,PROD_INSTNC_TYPE_TXT,PROD_INSTNC_STAT_TXT, PROD_INSTNC_ALIAS_TYPE_CD
ORDER BY PROD_INSTNC_MASTER_SRC_ID,PROD_INSTNC_TYPE_TXT 

SELECT 

PROD_INSTNC_STAT_CD,
PROD_INSTNC_STAT_TXT,
COUNT(*)
FROM `cto-wln-sa-data-pr-bb5283.ref_table.bq_hpbi_product_instance_profl` 
GROUP BY 
 PROD_INSTNC_STAT_CD,PROD_INSTNC_STAT_TXT

SELECT 

CUST_TYPE_CD,
CUST_TYPE_TXT,
COUNT(*)
FROM `cto-wln-sa-data-pr-bb5283.ref_table.bq_hpbi_product_instance_profl` 
GROUP BY CUST_TYPE_CD, CUST_TYPE_TXT

SELECT
  COUNT(*),
  PROD_INSTNC_ACTVY_TYPE_CD
FROM `cto-wln-sa-data-pr-bb5283.ref_table.bq_hpbi_product_instance_profl` 
GROUP BY PROD_INSTNC_ACTVY_TYPE_CD

SELECT 

BRAND_CD,
BRAND_TXT,
COUNT(*)
FROM `cto-wln-sa-data-pr-bb5283.ref_table.bq_hpbi_product_instance_profl` 
GROUP BY BRAND_CD, BRAND_TXT

-- MOBILITY BASE
SELECT 

  COUNT(*)
FROM `cto-wln-sa-data-pr-bb5283.ref_table.bq_hpbi_product_instance_profl` 
WHERE
  BRAND_CD = '1' -- 1 = Telus, 3 = Koodo, 7 = Public Mobile, 6 = PC Mobile
  AND CUST_TYPE_CD = 'R' -- CUST_TYPE_TXT: R = Customer type individual / Not Available, B = Business / Not Available
  AND PROD_INSTNC_TYPE_TXT='Cellular' 
  AND PROD_INSTNC_STAT_CD='A' -- PROD_INSTNC_STAT_TXT: A = Active, S = Suspended, C = Deactive, P = Pre-Activated


-- don't have account_type = 'I' and BAN_STATUS = 'OA'
