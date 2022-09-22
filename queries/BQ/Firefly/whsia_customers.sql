-- Find number of whsia customers that have stream+
-- As of April 29, 1815 customers have stream+ according to kb_service_agreement
-- As of April 29, 55 WHSIA customers have signed up for stream + 
SELECT COUNT(*)
FROM `cto-wln-sa-data-pr-bb5283.temp_workspace.firefly_customers_hive_kb_service_agreement` 
WHERE
  billing_account_number IN 
    (SELECT CAST(BACCT_BUS_BACCT_NUM AS STRING)
    FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` 
    WHERE 
      prod_instnc_ts = (SELECT MAX(prod_instnc_ts) FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` ) AND -- Get most recent date in snapshot table
      pp_bus_pp_catlg_itm_cd IN (SELECT whsia_soc FROM `cto-wln-sa-data-pr-bb5283.ref_table.bq_whsia_soc_codes`))  --Get WHSIA SOC Codes)
