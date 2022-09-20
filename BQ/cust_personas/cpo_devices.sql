-- CPO Devices in HPBI_PRODUCT_INSTANCE_PROFL
-- DEVICE_PROD_EQUIP_CD looks like the SKU Code

SELECT 
  BILLING_ACCOUNT_NUMBER, 
  PROD_INSTNC_RESRC_STR,
  PROD_INSTNC_ALIAS_STR,
  DEVICE_PROD_EQUIP_CD, 
  DEVICE_PROD_EQUIP_TXT, 
  TAC_ID, 
  DEVICE_MANUFACTURER_TXT, 
  BRAND_TXT, 
  PROVINCE_STATE_CD,
  TENURE_DAY_QTY,
    CASE WHEN 
      UPPER(DEVICE_PROD_EQUIP_CD) LIKE '%PRE%' OR 
      UPPER(DEVICE_PROD_EQUIP_CD) LIKE '%REFURB%' THEN 1 
      ELSE 0 
    END AS cpo_device
FROM `cto-wln-sa-data-pr-bb5283.ref_table.bq_hpbi_product_instance_profl` 
WHERE PROD_INSTNC_MASTER_SRC_ID=130

-- extrnl_sku_cd is the sku code 
SELECT * FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_ecomm_sku_ref` WHERE UPPER(extrnl_sku_cd) LIKE '%PRE%'

SELECT *  FROM `cto-wln-sa-data-pr-bb5283.ref_table.bq_exttable_hpbi_product_instance_profl` 
WHERE BILLING_ACCOUNT_NUMBER=36716759

SELECT * FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_hsehld_bacct_dly_snpsht` WHERE DATE(hsehld_bacct_dly_ts) = "2022-05-31" AND bus_bacct_num IN (36716759, 604169589)