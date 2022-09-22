-- Query for Naked Mobility Segment for Living Well Campaign

WITH NAKED_MOB_BASE AS (
  SELECT 
      MOB_BAN,
      MOB_CUST_ID,
      IMSI,
      MSISDN
      
  FROM cto-wln-sa-data-pr-bb5283.customer_personas_features.cust_wls_mnh_mapping_TB
  LEFT JOIN UNNEST(FFH_INFO) FFH_INFO
  WHERE
      --ACCOUNT_TYPE='R' -- ACCOUNT_TYPE is only NONE
      WHSIA_FLAG = False
      AND PAYMENT_MTHD = 'POST'
      AND MOB_OR_FFH = 'Naked Wireless'
),

DEVICE_TYPE_AGE AS (

  SELECT
      A.BILLING_ACCOUNT_NUMBER,
      A.PROD_INSTNC_RESRC_STR,
      A.PROD_INSTNC_ALIAS_STR,
      MAX(A.DEVICE_PROD_EQUIP_CD) AS DEVICE_PROD_EQUIP_CD,
      MAX(A.DEVICE_PROD_EQUIP_TXT) AS DEVICE_PROD_EQUIP_TXT,
      MAX(A.DEVICE_EQUIP_SERIAL_NUM) AS DEVICE_EQUIP_SERIAL_NUM,
      MAX(A.TAC_ID) AS TAC_ID,
      MAX(A.DEVICE_MANUFACTURER_TXT) AS DEVICE_MANUFACTURER_TXT,
      MAX(B.days_since_launch) AS days_since_launch
  FROM `cto-wln-sa-data-pr-bb5283.ref_table.bq_hpbi_product_instance_profl` A
  LEFT JOIN `cto-wln-sa-data-pr-bb5283.customer_personas_features.tac_info` B
  ON A.TAC_ID = B.tac_id
  GROUP BY A.BILLING_ACCOUNT_NUMBER, A.PROD_INSTNC_RESRC_STR, A.PROD_INSTNC_ALIAS_STR

),

POSTAL_CD AS (
  SELECT
    BUS_BILLING_ACCOUNT_NUM,
    MAX(POSTAL_CD) AS POSTAL_CD
  FROM `cto-wln-sa-data-pr-bb5283.ref_table.bq_hpbi_billing_account_dim` 
  GROUP BY BUS_BILLING_ACCOUNT_NUM
),

DEMOSTATS AS (
  SELECT  
    CODE AS POSTAL_CD,
    ECYBASHPOP AS household_pop,
    ECYHTA4044 AS household_pop_40_to_44,
    ECYHTA4549 AS household_pop_45_to_49,
    ECYHTA5054 AS household_pop_50_to_54,
    ECYHTA5559 AS household_pop_55_to_59,
    ECYHTA6064 AS household_pop_60_to_64,
    ECYHTA6569 AS household_pop_65_to_69,
    ECYHTA7074 AS household_pop_70_to_74,
    ECYHTA7579 AS household_pop_75_to_79,
    ECYHTA8084 AS household_pop_80_to_84,
    ECYHTA85P AS household_pop_85_or_greater
  FROM `cto-wln-sa-data-pr-bb5283.demostats_2021.21_geo` 
  WHERE
      GEO = 'FSALDU'
),

PRIZM AS (
  SELECT
    fsaldu AS POSTAL_CD,
    sgname,
    lsname,
    Avg_Income,
    Income_Level
  FROM `cto-wln-sa-data-pr-bb5283.temp_workspace.income_levels` 

),

TENURE AS (
  SELECT
    bacct_bus_bacct_num,
    bacct_bus_bacct_num_src_id,
    DATE_DIFF(CURRENT_DATE() , MIN(bacct_bacct_create_ts) , DAY) AS tenure_days
  FROM 
      `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht`
  WHERE
      prod_instnc_ts = (SELECT MAX(prod_instnc_ts) FROM  `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht`)
  GROUP BY bacct_bus_bacct_num,    bacct_bus_bacct_num_src_id
)

SELECT
  t1.MOB_BAN,
  t1.MOB_CUST_ID,
  t1.IMSI,
  t1.MSISDN,
  t2.POSTAL_CD,
  t3.* except (POSTAL_CD),
  t4.* except (POSTAL_CD),
  t5.tenure_days,
  t6.days_since_launch,
  t6.DEVICE_MANUFACTURER_TXT
  
FROM NAKED_MOB_BASE t1 
LEFT JOIN POSTAL_CD t2 -- Get Postal Code 
ON t1.MOB_BAN = t2.BUS_BILLING_ACCOUNT_NUM
LEFT JOIN DEMOSTATS t3
ON t2.POSTAL_CD = t3.POSTAL_CD 
LEFT JOIN PRIZM t4
ON t2.POSTAL_CD = t4.POSTAL_CD
LEFT JOIN TENURE t5
ON t1.MOB_BAN = t5.bacct_bus_bacct_num AND t1.MOB_CUST_ID = t5.bacct_bus_bacct_num_src_id
LEFT JOIN DEVICE_TYPE_AGE t6 
ON t1.MOB_BAN = t6.BILLING_ACCOUNT_NUMBER AND t1.MSISDN = t6.PROD_INSTNC_RESRC_STR AND t1.IMSI=t6.PROD_INSTNC_ALIAS_STR

