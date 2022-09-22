
-- Query to create dataset for LWC Campaign

--------------------------- TV USAGE ---------------------------

DECLARE tv_media_categories_pivot_str STRING;

EXECUTE IMMEDIATE """
  CREATE TEMP TABLE TEMP_TV_USAGE_STAGE AS 
    WITH usage AS (
      SELECT 
        tv_pgm_watch_dly_sum_dt,
        CASE WHEN EXTRACT(DAYOFWEEK FROM tv_pgm_watch_dly_sum_dt) IN (1, 7) THEN "weekend"  ELSE "weekday" END AS day,
        bus_bacct_num,
        bus_bacct_num_src_id,
        --CAST(bus_bacct_num AS STRING) AS bus_bacct_num,
        --CAST(bus_bacct_num_src_id AS INTEGER) AS bus_bacct_num_src_id,
        REGEXP_REPLACE(TRIM(LOWER(tv_media_subcatgy_txt)),r'[^a-zA-Z]','') AS tv_media_subcatgy_txt,
        tv_watch_tot_sec_qty/ 60.0 AS tv_watch_tot_mins,
        --SUM(tv_watch_tot_cnt) AS tv_watch_tot_cnt,

      FROM `cio-datahub-enterprise-pr-183a.ent_usage_rated_tv.bq_tv_program_watch_dly_view`
      WHERE 
        -- bus_bacct_num=604690167
        tv_pgm_watch_dly_sum_dt > '2022-03-29'
        AND tv_media_subcatgy_txt is NOT NULL 
    )
    SELECT 
      bus_bacct_num,
      bus_bacct_num_src_id,
      CONCAT(tv_media_subcatgy_txt, '_', day) AS category_col,
      SUM(tv_watch_tot_mins) AS total_tv_mins
    FROM usage
    GROUP BY 
      CONCAT(tv_media_subcatgy_txt, '_', day), bus_bacct_num, bus_bacct_num_src_id
  """ ;
SET tv_media_categories_pivot_str = (SELECT CONCAT('("', STRING_AGG(DISTINCT category_col, '", "'), '")') FROM TEMP_TV_USAGE_STAGE);

EXECUTE IMMEDIATE format("""
    CREATE TEMP TABLE TEMP_TV_USAGE AS(
    SELECT 
      * 
    FROM 
      TEMP_TV_USAGE_STAGE
    PIVOT(
      MAX(total_tv_mins) AS tv_watch_tot_mins
      FOR category_col IN %s)
    )
  """, tv_media_categories_pivot_str );

--------------------------- Naked FFH Customer Base ---------------------------

WITH CUST_BASE AS (

    SELECT 
        FFH_BAN,
        FFH_CUST_ID
    FROM `cto-wln-sa-data-pr-bb5283.customer_personas_features.cust_wln_mnh_mapping`
    LEFT JOIN UNNEST(MOB_INFO) MOB_INFO
    WHERE MOB_OR_FFH = 'Naked Wireline'
    GROUP BY FFH_BAN, FFH_CUST_ID

),

--------------------------- Get Product Mix ---------------------------

HOUSEHOLD_PRODUCTS AS (
  SELECT
    bus_bacct_num,
    bus_bacct_num_src_id,
    hsehld_src_id,
    MAX(hsehld_actv_party_cnt) AS hsehld_actv_party_cnt,
    MAX(hsehld_actv_hsic_srvc_cnt) AS hsehld_actv_hsic_srvc_cnt,
    MAX(hsehld_actv_dial_up_srvc_cnt) AS hsehld_actv_dial_up_srvc_cnt,
    MAX(hsehld_actv_hm_ph_srvc_cnt) AS hsehld_actv_hm_ph_srvc_cnt,
    MAX(hsehld_actv_smart_hm_srvc_cnt) AS hsehld_actv_smart_hm_srvc_cnt,
    MAX(hsehld_actv_post_mbl_srvc_cnt) AS hsehld_actv_post_mbl_srvc_cnt,
    MAX(hsehld_actv_pre_mbl_srvc_cnt) AS hsehld_actv_pre_mbl_srvc_cnt,
    MAX(hsehld_actv_optc_tv_srvc_cnt) AS hsehld_actv_optc_tv_srvc_cnt,
    MAX(hsehld_actv_sat_tv_srvc_cnt) AS hsehld_actv_sat_tv_srvc_cnt,
    MAX(hsehld_actv_oth_srvc_cnt) AS hsehld_actv_oth_srvc_cnt,
    MAX(hsehld_canc_bacct_cnt) AS hsehld_canc_bacct_cnt,
    MAX(hsehld_suspnd_bacct_cnt) AS hsehld_suspnd_bacct_cnt, 
    MAX(hsehld_actv_bacct_cnt) AS hsehld_actv_bacct_cnt,
    MAX(hsehld_actv_wls_srvc_only_ind) AS hsehld_actv_wls_srvc_only_ind,
    MAX(hsehld_actv_wln_srvc_only_ind) AS hsehld_actv_wln_srvc_only_ind,
    MAX(hsehld_actv_wln_srvc_cnt) AS hsehld_actv_wln_srvc_cnt,
    MAX(hsehld_actv_wls_koodo_srvc_cnt) AS hsehld_actv_wls_koodo_srvc_cnt,
    MAX(hsehld_actv_wls_telus_srvc_cnt) AS hsehld_actv_wls_telus_srvc_cnt
  FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_hsehld_bacct_dly_snpsht` 
  WHERE 
      DATE(hsehld_bacct_dly_ts) = (SELECT MAX(DATE(hsehld_bacct_dly_ts)) FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_hsehld_bacct_dly_snpsht` )
      AND bacct_stat_cd='O'
      AND bus_bacct_num IS NOT NULL
  GROUP BY bus_bacct_num, bus_bacct_num_src_id, hsehld_src_id
),

--------------------------- Get Postal Code of each BAN ---------------------------


BAN_DIM_POSTAL_CODE AS (
  SELECT
    BUS_BILLING_ACCOUNT_NUM,
    MAX(POSTAL_CD) AS POSTAL_CD,
    MAX(PROVINCE) AS PROVINCE,
    MAX(CITY) AS CITY,
  FROM `cto-wln-sa-data-pr-bb5283.ref_table.bq_hpbi_billing_account_dim` 
  GROUP BY BUS_BILLING_ACCOUNT_NUM
),
---------------------------  DEMOSTATS Data is at a Postal Code Level ---------------------------

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

---------------------------  PRIZM Data is at a Postal Code Level ---------------------------

PRIZM_DATA AS ( 
  SELECT
    fsaldu AS POSTAL_CD,
    sgname,
    lsname,
    Avg_Income,
    Income_Level
  FROM `cto-wln-sa-data-pr-bb5283.temp_workspace.income_levels` 

),

--------------------------- Clickstream Data for LWC Products ---------------------------

UUID_BAN_TABLE AS (
    SELECT
        CAST(bus_bacct_num AS STRING) AS BAN,
        CAST(bus_bacct_src_id AS STRING) AS bus_bacct_src_id,
        uuid,
        bus_cust_id,
        bus_cust_src_id,
        consldt_bus_cust_id,
        consldt_bus_cust_src_id
    FROM
        `cio-datahub-enterprise-pr-183a.ent_party_identity.bq_cust_idnty_billg_acct_snpsht`
    WHERE
        cust_idnty_billg_acct_ts = (select max(cust_idnty_billg_acct_ts) FROM `cio-datahub-enterprise-pr-183a.ent_party_identity.bq_cust_idnty_billg_acct_snpsht`)
        ORDER BY bus_bacct_num,uuid DESC
    ),
    
CLICKSTREAM_UUID_TABLE AS (
    SELECT 
      uuid,
      COUNT(DISTINCT post_evar100_txt) as num_distinct_pages_visited
    FROM `cio-datahub-enterprise-pr-183a.ent_cust_intractn.bq_clckstrm_telus_web_event`
    WHERE 
      DATE(clckstrm_event_ts) > '2022-03-29' AND
      regexp_contains(post_evar100_txt, r".*livingwell-companion") AND
      uuid <> 'n/avail'
    GROUP BY uuid
    ),
  
LWC_CLICKSTREAM_TABLE AS (
  SELECT
    CAST(t1.BAN AS INTEGER) AS BAN, 
    CAST(t1.bus_bacct_src_id AS INTEGER) AS bus_bacct_src_id,
    SUM(t2.num_distinct_pages_visited) as num_distinct_pages_visited
  FROM
      UUID_BAN_TABLE t1
  RIGHT JOIN CLICKSTREAM_UUID_TABLE t2
  ON t1.uuid = t2.uuid
  GROUP BY t1.BAN, t1.bus_bacct_src_id
  ),


--------------------------- Customers with LWC Product ---------------------------

LWC_ACCOUNTS AS (
  SELECT 
    bus_prod_instnc_id,
    bus_bacct_num_src_id,
    bus_bacct_num ,
    pi_prod_instnc_stat_cd, -- Status of the product instance - A = Active, O = open, C = Cancelled
    pi_prim_pp_sls_start_ts AS product_start_ts,
    pi_cntrct_start_ts, 
    pi_cntrct_end_ts,
    pi_cntrct_durtn_mth_qty,
    bus_pp_catlg_itm_cd, 
    bus_pp_catlg_itm_nm
  FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_subscn_item_profl_snpsht` 
  WHERE snpsht_dt = (SELECT MAX(snpsht_dt) FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_subscn_item_profl_snpsht` )
  AND bus_pp_catlg_itm_cd IN  ('40797411', '40797421' , '40797431' , '40797441' , '40797811' , '41083751' , '40800491', '41086361', '41093531', '41093541' , '41161911', '41161921', '41161931' )
),

LWC_BASE_TABLE AS (
  SELECT
    bus_bacct_num,
    bus_bacct_num_src_id,
    COUNT(bus_pp_catlg_itm_nm) AS num_lwc_products
  FROM LWC_ACCOUNTS
  GROUP BY bus_bacct_num, bus_bacct_num_src_id
),

TENURE AS (
  SELECT
    bacct_bus_bacct_num,
    DATE_DIFF(CURRENT_DATE() , MIN(bacct_bacct_create_ts) , DAY) AS tenure_days
  FROM 
      `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht`
  WHERE
      prod_instnc_ts = (SELECT MAX(prod_instnc_ts) FROM  `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht`)
  GROUP BY bacct_bus_bacct_num   
)

--------------------------- Merge Tables Together ---------------------------

SELECT 
  t1.*,
  t2.* except(bus_bacct_num, bus_bacct_num_src_id),
  t3.* except(bus_bacct_num, bus_bacct_num_src_id),
  t4.* except(BAN, bus_bacct_src_id),
  t5.* except(BUS_BILLING_ACCOUNT_NUM),
  t6.* except(POSTAL_CD),
  t7.* except(bus_bacct_num, bus_bacct_num_src_id),
  t8.* except(POSTAL_CD),
  t9.tenure_days

FROM CUST_BASE t1
LEFT JOIN HOUSEHOLD_PRODUCTS t2 
ON t1.FFH_BAN = t2.bus_bacct_num 
LEFT JOIN TEMP_TV_USAGE t3
ON t1.FFH_BAN = t3.bus_bacct_num 
LEFT JOIN LWC_CLICKSTREAM_TABLE t4 
ON t1.FFH_BAN = t4.BAN 
LEFT JOIN BAN_DIM_POSTAL_CODE t5 
ON t1.FFH_BAN = t5.BUS_BILLING_ACCOUNT_NUM 
LEFT JOIN PRIZM_DATA t6 
ON t5.POSTAL_CD = t6.POSTAL_CD
LEFT JOIN LWC_BASE_TABLE t7
ON t1.FFH_BAN = t7.bus_bacct_num 
LEFT JOIN DEMOSTATS t8
ON t5.POSTAL_CD = t8.POSTAL_CD
LEFT JOIN TENURE t9
ON t1.FFH_BAN = t9.bacct_bus_bacct_num

