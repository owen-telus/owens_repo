  WITH MOB_POSTPAID_BASE AS(

  SELECT 
    A.bacct_bus_bacct_num AS BAN,
    A.pi_prod_instnc_resrc_str AS MSISDN,
    B.prod_instnc_alias_str AS IMSI,

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
    --AND B.prod_instnc_alias_str = '302220023511271'
),

APP_USAGE_STAGE  AS (
  SELECT
    A.BAN,
    A.MSISDN,
    A.IMSI,
    
    CASE WHEN EXTRACT(DAYOFWEEK FROM B.event_dt) IN (1,7) THEN 'weekend'
    ELSE 'weekday'
    END AS day,
    C.tier_1,
    ROUND(SUM(SAFE_DIVIDE(B.ul_volume_qty + B.dl_volume_qty, 1000000.0)), 3) AS usage_volume_MB
  FROM MOB_POSTPAID_BASE A  
  LEFT JOIN `cio-datahub-enterprise-pr-183a.ent_usage_unrated_ott.bq_ott_app_event` B 
  ON A.IMSI = B.imsi_num 
  INNER JOIN `bi-srv-aaaie-pr-c0c268.ref_table.bq_app_cat_mapping` C 
  ON B.app_nm = C.app_nm 
  WHERE
    C.tier_1 IN ('Communication', 'Entertainment', 'Finance', 'Gaming', 'Lifestyle', 'News', 'OS', 'Productivity', 'Shopping', 'Smart Home', 'Telecom', 'Travel', 'Real Estate', 'Pets')
    AND B.event_dt >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)
  GROUP BY  A.BAN, A.MSISDN,A.IMSI,day, C.tier_1
),


APP_USAGE AS (
  SELECT
    A.BAN,
    A.MSISDN,
    A.IMSI,
    CONCAT(REGEXP_REPLACE(TRIM(LOWER(A.tier_1)),r'[^a-zA-Z]',''), '_', day) AS category,
    A.usage_volume_MB
  FROM APP_USAGE_STAGE A

),

APP_USAGE_JOINED AS (
  SELECT 
    *
  FROM APP_USAGE
  PIVOT(
    MAX(usage_volume_MB) AS usage_volume_MB
    FOR category IN ('communication_weekday', 'entertainment_weekday', 'finance_weekday', 'gaming_weekday', 'lifestyle_weekday', 'news_weekday', 'os_weekday', 'productivity_weekday', 'shopping_weekday', 'smarthome_weekday', 'telecom_weekday', 'travel_weekday', 'realestate_weekday', 'pets_weekday',
  'communication_weekend', 'entertainment_weekend', 'finance_weekend', 'gaming_weekend', 'lifestyle_weekend', 'news_weekend', 'os_weekend', 'productivity_weekend', 'shopping_weekend', 'smarthome_weekend', 'telecom_weekend', 'travel_weekend', 'realestate_weekend', 'pets_weekend')
  )
),

NUM_MARKETING_MESSAGES_RECEIVED AS (
  SELECT 
    subscr_num AS MSISDN,
    COUNT(*) AS num_marketing_messages_last_month
  FROM `cio-datahub-enterprise-pr-183a.ent_cust_intractn.bq_mob_dbm` 
  WHERE 
    cmpgn_in_hm_dt >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)
  GROUP BY MSISDN

),


NUM_THROTTLE_DAYS_STAGE AS (

  SELECT
    ban AS BAN,
    subscr_ph_num AS MSISDN,
    data_usg_dly_sum_dt,
    bill_cycle_clos_dt,
    wisp_chg_amt,
    wisp_actl_kb_qty,
    CASE WHEN throttle_ind = 'Y' 
    THEN 1 
    ELSE 0 END AS throttle_bool,
    DENSE_RANK() OVER(PARTITION BY ban, subscr_ph_num ORDER BY billg_cycl_yr_num DESC, billg_cycl_mth_num DESC) as row_num -- 1 will be most recent cycle
    
  FROM `cio-datahub-enterprise-pr-183a.ent_usage_rated.bq_wls_data_usg_dly_sum` 
  --WHERE subscr_ph_num=  '5879838001' --   '2043339222' -- something wrong with this table, there is a delay
  --ORDER BY data_usg_dly_sum_dt DESC
         

),

NUM_THROTTLE_DAYS AS (

  SELECT 
    BAN,
    MSISDN,
    row_num,
    MAX(bill_cycle_clos_dt) AS bill_cycle_clos_dt,
    SUM(throttle_bool) AS num_days_throttled,
    SUM(wisp_chg_amt) AS data_overcharge_amt,
    MAX(wisp_actl_kb_qty /1000000.0) AS max_daily_data_used_gb,
    SUM(wisp_actl_kb_qty /1000000.0) AS data_used_gb
  FROM NUM_THROTTLE_DAYS_STAGE
  WHERE row_num <= 4
  GROUP BY BAN, MSISDN, row_num
),

NUM_THROTTLE_DAYS_PIVOTED AS (

  SELECT *
  FROM NUM_THROTTLE_DAYS
  PIVOT (
    MAX(bill_cycle_clos_dt) AS bill_cycle_clos_dt,
    MAX(num_days_throttled) AS num_days_throttled,
    MAX(data_overcharge_amt) AS data_overcharge_amt,
    MAX(max_daily_data_used_gb) AS max_daily_data_used_gb,
    MAX(data_used_gb) AS data_used_gb
    FOR row_num IN (1,2,3, 4) 
  )
),

NUM_THROTTLE_DAYS_PIVOTED_DAYS AS (

  SELECT 
    *,
    CASE WHEN CURRENT_DATE() >= bill_cycle_clos_dt_2 
    THEN DATE_DIFF(bill_cycle_clos_dt_1 , bill_cycle_clos_dt_2, DAY) 
    ELSE DATE_DIFF(CURRENT_DATE() , bill_cycle_clos_dt_2, DAY) -- Else in current cycle and use current_date to get days used in this cycle
    END AS current_period_days,

    DATE_DIFF(bill_cycle_clos_dt_2 , bill_cycle_clos_dt_3, DAY) AS second_period_days,
    DATE_DIFF(bill_cycle_clos_dt_3 , bill_cycle_clos_dt_4, DAY) AS third_period_days

  FROM NUM_THROTTLE_DAYS_PIVOTED
),

WLS_DATA_USG_FEATURES AS (
  SELECT 

    BAN,
    MSISDN,
    data_overcharge_amt_1,
    data_overcharge_amt_2,
    data_overcharge_amt_3,
    max_daily_data_used_gb_1,
    max_daily_data_used_gb_2,
    max_daily_data_used_gb_3,
    data_used_gb_1, -- Current t Month
    data_used_gb_2, -- t-1 Month
    data_used_gb_3, -- t-2 Month
    CASE WHEN num_days_throttled_1 > current_period_days
    THEN 1 
    ELSE num_days_throttled_1 / current_period_days 
    END AS perc_days_throttled_1, 

    CASE WHEN num_days_throttled_2 > second_period_days
    THEN 1 
    ELSE num_days_throttled_2 / second_period_days 
    END AS perc_days_throttled_2, 

    CASE WHEN num_days_throttled_3 > third_period_days
    THEN 1 
    ELSE num_days_throttled_3 / third_period_days 
    END AS perc_days_throttled_3, 

    FROM NUM_THROTTLE_DAYS_PIVOTED_DAYS

),

PROD_INSTNC_SNPSHT_FEATURES AS (
  
  SELECT 
    A.bacct_bus_bacct_num AS BAN,
    A.pi_prod_instnc_resrc_str AS MSISDN,
    B.prod_instnc_alias_str AS IMSI,
    A.pp_recur_chrg_amt,
    A.bacct_delinq_ind,
    A.bacct_ebill_ind,
    A.pp_bus_pp_catlg_itm_cd, 
    A.pp_catlg_itm_nm
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

),

HSEHLD_LEVEL_PRODUCT_MIX AS (
  SELECT
    bus_bacct_num AS BAN,
    -- seems to be duplicates, do a group by
    SUM(hsehld_actv_hsic_srvc_cnt) AS hsehld_actv_hsic_srvc_cnt,
    SUM(hsehld_actv_smart_hm_srvc_cnt) AS hsehld_actv_smart_hm_srvc_cnt,
    SUM(hsehld_actv_post_mbl_srvc_cnt) AS hsehld_actv_post_mbl_srvc_cnt,
    SUM(hsehld_actv_optc_tv_srvc_cnt) AS hsehld_actv_optc_tv_srvc_cnt,
    MAX(hsehld_actv_wls_srvc_only_ind) AS hsehld_actv_wls_srvc_only_ind,
    SUM(hsehld_actv_wln_srvc_cnt) AS hsehld_actv_wln_srvc_cnt,
    SUM(hsehld_actv_wls_koodo_srvc_cnt) AS hsehld_actv_wls_koodo_srvc_cnt,
    SUM(hsehld_actv_wls_telus_srvc_cnt) AS hsehld_actv_wls_telus_srvc_cnt
  FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_hsehld_bacct_dly_snpsht`
  WHERE DATE(hsehld_bacct_dly_ts) =  CURRENT_DATE() - 1 
  GROUP BY BAN 
)

SELECT 
  A.*,
  B.num_marketing_messages_last_month,
  C.* EXCEPT(BAN, MSISDN),
  D.* EXCEPT(BAN, MSISDN, IMSI),
  E.* EXCEPT(BAN)

FROM APP_USAGE_JOINED A 
LEFT JOIN NUM_MARKETING_MESSAGES_RECEIVED B 
ON A.MSISDN = B.MSISDN
LEFT JOIN WLS_DATA_USG_FEATURES C 
ON A.BAN = C.BAN AND A.MSISDN = C.MSISDN
LEFT JOIN PROD_INSTNC_SNPSHT_FEATURES D 
ON A.BAN = D.BAN AND A.MSISDN = D.MSISDN AND A.IMSI = D.IMSI 
LEFT JOIN HSEHLD_LEVEL_PRODUCT_MIX E
ON A.BAN = E.BAN


