-- As of May 3 2022, following query returns 3,846,828 customers
-- 3,846,789 Distinct IMSI's
WITH mobility_customer_base AS (
  SELECT 
    BILLING_ACCOUNT_NUMBER AS BAN,
    PROD_INSTNC_RESRC_STR AS MSISDN,
    PROD_INSTNC_ALIAS_STR AS IMSI,
    PRIM_PRICE_PLAN_CD,
    PRIM_PRICE_PLAN_TXT,
    TAC_ID
  FROM `cto-wln-sa-data-pr-bb5283.ref_table.bq_hpbi_product_instance_profl` 
  WHERE
    BRAND_CD = '1' -- 1 = Telus, 3 = Koodo, 7 = Public Mobile, 6 = PC Mobile
    AND CUST_TYPE_CD = 'R' -- CUST_TYPE_TXT: R = Customer type individual / Not Available, B = Business / Not Available
    AND PROD_INSTNC_TYPE_TXT='Cellular' 
    AND PROD_INSTNC_STAT_CD='A' -- PROD_INSTNC_STAT_TXT: A = Active, S = Suspended, C = Deactive, P = Pre-Activated
    AND PROD_INSTNC_ALIAS_STR IS NOT NULL 
    AND PRIM_PRICE_PLAN_CD NOT IN (SELECT whsia_soc FROM `cto-wln-sa-data-pr-bb5283.ref_table.bq_whsia_soc_codes`) -- Filter out WHSIA Customers
),

mobility_price_plan AS (
  SELECT
    BACCT_BUS_BACCT_NUM AS BAN,
    pi_prod_instnc_resrc_str AS MSISDN,
    pp_recur_chrg_amt,
    pp_bus_pp_catlg_itm_cd -- should be the same as PRIM_PRICE_PLAN_CD
  FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` 
  WHERE 
    prod_instnc_ts = (SELECT MAX(prod_instnc_ts) FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` )  -- Get most recent date in snapshot table
    AND bacct_brand_id=1 
    AND pi_prod_instnc_typ_cd = 'C'
    AND bacct_bacct_typ_cd = 'I'
    AND bacct_bacct_subtyp_cd = 'R' 
    AND bacct_bacct_stat_cd = 'O'
),

joined_table AS (
  SELECT
    A.BAN,
    A.MSISDN,
    A.IMSI,
    A.PRIM_PRICE_PLAN_CD,
    A.PRIM_PRICE_PLAN_TXT,
    B.is_5g_capable,
    B.is_4g_capable,
    B.days_since_launch,
    B.market_type,
    C.COUNT_DISTINCT_TAC,
    D.pp_recur_chrg_amt,
    D.pp_bus_pp_catlg_itm_cd,
    E.FSA_CODE,
    E.value_avg_total_income AS FSA_avg_total_income,
    E.value_median_total_income AS FSA_median_total_income,

    CASE 
      WHEN TRIM(A.PRIM_PRICE_PLAN_TXT) LIKE '%5G+%' THEN '5G+'
      WHEN TRIM(A.PRIM_PRICE_PLAN_TXT) LIKE '%5G %' THEN '5G'
      ELSE 'None'
    END AS plan_5G_5G_plus

  FROM mobility_customer_base A
  LEFT JOIN `cto-wln-sa-data-pr-bb5283.customer_personas_features.tac_info` B ON A.TAC_ID = B.TAC_ID
  LEFT JOIN `cto-wln-sa-data-pr-bb5283.customer_personas_features.cust_tacs_count` C ON A.BAN = C.BAN AND A.MSISDN = C.MSISDN 
  LEFT JOIN mobility_price_plan D ON A.BAN = D.BAN AND A.MSISDN = D.MSISDN
  LEFT JOIN `cto-wln-sa-data-pr-bb5283.customer_personas_features.statscan_fsa_income_per_ban` E ON A.BAN = E.BUS_BILLING_ACCOUNT_NUM

)

SELECT
  COUNT(*) AS DUPLICATES, MSISDN 
FROM joined_table
GROUP BY MSISDN
ORDER BY DUPLICATES DESC 



