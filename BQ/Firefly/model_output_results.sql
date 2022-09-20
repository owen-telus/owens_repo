-- SELECT COUNT(*)
-- FROM `cto-wln-sa-data-pr-bb5283.temp_workspace.firefly_customers_hive_kb_service_agreement` 
-- WHERE
--   billing_account_number IN 
--     (SELECT CAST(BACCT_BUS_BACCT_NUM AS STRING)
--     FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` 
--     WHERE 
--       prod_instnc_ts = (SELECT MAX(prod_instnc_ts) FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` ) AND -- Get most recent date in snapshot table
--       pp_bus_pp_catlg_itm_cd IN (SELECT whsia_soc FROM `cto-wln-sa-data-pr-bb5283.ref_table.bq_whsia_soc_codes`))  --Get WHSIA SOC Codes)

WITH firefly_output_with_BAN AS (
  SELECT
    A.*,
    B.MSISDN,
    B.MOB_BAN 
  FROM `cto-wln-sa-data-pr-bb5283.customer_personas_reports.firefly_campaign_output` A 
  LEFT JOIN `cto-wln-sa-data-pr-bb5283.customer_personas_features.cust_wls_mnh_mapping` B 
  ON A.IMSI = B.IMSI
),



has_firefly_table AS (SELECT 
  A.IMSI,
  A.MOB_BAN,
  A.MSISDN,
  A.propensity_score_pctl,
  CASE WHEN B.soc_effective_ts IS NULL THEN 'N' ELSE 'Y' END AS has_firefly
FROM firefly_output_with_BAN A 
LEFT JOIN `cto-wln-sa-data-pr-bb5283.temp_workspace.firefly_customers_hive_kb_service_agreement` B -- customers with firefly
ON CAST(A.MOB_BAN AS STRING) = B.billing_account_number
)
-- SELECT 
--   COUNT(*)
-- FROM firefly_output_with_BAN
-- WHERE MOB_BAN IS NULL -- 9530 customers with BAN NULL

-- 541 customers with firefly. What about the other 1300 or so customers? Did their BAN not get mapped or was it not included in the initial analysis?

-- SELECT
--   has_firefly,
--   COUNT(*)
-- FROM has_firefly_table
-- GROUP BY has_firefly 


SELECT
  has_firefly,
  COUNT(*) as num_customers,
  AVG(propensity_score_pctl) as avg_propensity
  
  
FROM has_firefly_table

WHERE has_firefly = 'Y'
GROUP BY has_firefly
