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


TIME_INDEX_STAGE AS (

  SELECT
    A.*,
    dt,
    DATE_TRUNC(dt, WEEK(MONDAY)) AS week_start_dt, -- This gets the Monday of that week (start_date)
    DATE_ADD(DATE_TRUNC(dt, WEEK(MONDAY)), INTERVAL 4 DAY) AS week_end_dt,
    
    -- Only when dt is not on a Sunday, add a week
    CASE WHEN
      EXTRACT(DAYOFWEEK FROM dt) != 1 THEN DATE_ADD(DATE_TRUNC(dt, WEEK(SUNDAY)), INTERVAL 1 WEEK)  -- 1 = Sunday, 7 = Saturday
      ELSE DATE_TRUNC(dt, WEEK(SUNDAY))
    END AS end_dt, 

  -- Regardless of Start Date, query is now robust to have the same start and end date on Monday -> Sunday regardless of when query is ran
  -- Get past month Monday / Friday 

  FROM MOB_POSTPAID_BASE A,
  UNNEST(GENERATE_DATE_ARRAY(DATE_SUB(CURRENT_DATE(),INTERVAL 4 WEEK), CURRENT_DATE(), INTERVAL 1 WEEK)) as dt 

),

BASE_TIME_INDEX AS (
  SELECT 
    BAN,
    MSISDN,
    IMSI,
    
    RANK() OVER(PARTITION BY BAN, MSISDN, IMSI ORDER BY dt desc) AS ts_index,
    dt,
    week_start_dt,
    week_end_dt,
    DATE_SUB(end_dt, INTERVAL 1 DAY) AS weekend_start_dt,
    end_dt AS weekend_end_dt
  FROM (
    SELECT * FROM TIME_INDEX_STAGE WHERE end_dt < CURRENT_DATE()
  )
),


APP_USAGE_WEEKDAY AS (
  SELECT
    A.BAN,
    A.MSISDN,
    A.IMSI,
    A.ts_index,
    A.week_start_dt,
    A.week_end_dt,
    CONCAT(REGEXP_REPLACE(TRIM(LOWER(tier_1)),r'[^a-zA-Z]',''), '_', 'weekday') AS category,
    ROUND(SUM(SAFE_DIVIDE(B.ul_volume_qty + B.dl_volume_qty, 1000000.0)), 3) AS usage_volume_MB
  FROM BASE_TIME_INDEX A  
  LEFT JOIN `cio-datahub-enterprise-pr-183a.ent_usage_unrated_ott.bq_ott_app_event` B 
  ON A.IMSI = B.imsi_num 
  AND B.event_dt >= A.week_start_dt AND B.event_dt <= A.week_end_dt 
  INNER JOIN `bi-srv-aaaie-pr-c0c268.ref_table.bq_app_cat_mapping` C 
  ON B.app_nm = C.app_nm 
  WHERE
    C.tier_1 IN ('Communication', 'Entertainment', 'Finance', 'Gaming', 'Lifestyle', 'News', 'OS', 'Productivity', 'Shopping', 'Smart Home', 'Telecom', 'Travel', 'Real Estate', 'Pets')
    AND B.event_dt > DATE_SUB(CURRENT_DATE(), INTERVAL 5 WEEK)
  GROUP BY 1,2,3,4,5,6,7


),

APP_USAGE_WEEKEND AS (
  SELECT
    A.BAN,
    A.MSISDN,
    A.IMSI,
    A.ts_index,
    A.weekend_start_dt,
    A.weekend_end_dt,
    CONCAT(REGEXP_REPLACE(TRIM(LOWER(tier_1)),r'[^a-zA-Z]',''), '_', 'weekend') AS category,
    ROUND(SUM(SAFE_DIVIDE(B.ul_volume_qty + B.dl_volume_qty, 1000000.0)), 3) AS usage_volume_MB
  FROM BASE_TIME_INDEX A  
  LEFT JOIN `cio-datahub-enterprise-pr-183a.ent_usage_unrated_ott.bq_ott_app_event` B 
  ON A.IMSI = B.imsi_num 
  AND B.event_dt >= A.weekend_start_dt AND B.event_dt <= A.weekend_end_dt 
  INNER JOIN `bi-srv-aaaie-pr-c0c268.ref_table.bq_app_cat_mapping` C 
  ON B.app_nm = C.app_nm 
  WHERE
    C.tier_1 IN ('Communication', 'Entertainment', 'Finance', 'Gaming', 'Lifestyle', 'News', 'OS', 'Productivity', 'Shopping', 'Smart Home', 'Telecom', 'Travel', 'Real Estate', 'Pets')
    AND B.event_dt > DATE_SUB(CURRENT_DATE(), INTERVAL 5 WEEK)
  GROUP BY 1,2,3,4,5,6,7


),


APP_USAGE_WEEKDAY_PIVOTED AS(
  SELECT * 
  FROM APP_USAGE_WEEKDAY 
  PIVOT(
    MAX(usage_volume_MB) AS usage_volume_MB 
    FOR category IN ('communication_weekday', 'entertainment_weekday', 'finance_weekday', 'gaming_weekday', 'lifestyle_weekday', 'news_weekday', 'os_weekday', 'productivity_weekday', 'shopping_weekday', 'smarthome_weekday', 'telecom_weekday', 'travel_weekday', 'realestate_weekday', 'pets_weekday')
  )
),

APP_USAGE_WEEKEND_PIVOTED AS(
  SELECT * 
  FROM APP_USAGE_WEEKEND 
  PIVOT(
    MAX(usage_volume_MB) AS usage_volume_MB 
    FOR category IN ('communication_weekend', 'entertainment_weekend', 'finance_weekend', 'gaming_weekend', 'lifestyle_weekend', 'news_weekend', 'os_weekend', 'productivity_weekend', 'shopping_weekend', 'smarthome_weekend', 'telecom_weekend', 'travel_weekend', 'realestate_weekend', 'pets_weekend')
  )
)


SELECT 
  A.*,
  B.* EXCEPT(BAN, MSISDN, IMSI)
FROM APP_USAGE_WEEKDAY_PIVOTED A
FULL OUTER JOIN APP_USAGE_WEEKEND_PIVOTED B 
ON A.BAN = B.BAN AND A.MSISDN = B.MSISDN AND A.IMSI = B.IMSI AND A.ts_index = B.ts_index 