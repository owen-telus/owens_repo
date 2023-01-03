-- This script creates the initial weekly ts app usage table
-- All I need to do is create this initial table, and then create stored procedure for weekly updates
CREATE OR REPLACE TABLE `cto-wln-sa-data-pr-bb5283.customer_personas_features.imsi_app_usage_trends_weekly_ts_2` AS
WITH time_index_stage AS (
  SELECT
    dt,
    DATE_TRUNC(dt, WEEK(MONDAY)) AS start_dt, -- This gets the Monday of that week (start_date)
    
    -- Only when dt is not on a Sunday, add a week
    CASE WHEN
      EXTRACT(DAYOFWEEK FROM dt) != 1 THEN DATE_ADD(DATE_TRUNC(dt, WEEK(SUNDAY)), INTERVAL 1 WEEK)  -- 1 = Sunday, 7 = Saturday
      ELSE DATE_TRUNC(dt, WEEK(SUNDAY))
    END AS end_dt, 
  -- Regardless of Start Date, query is now robust to have the same start and end date on Monday -> Sunday regardless of when query is ran
  -- Change first date to min(event_dt) from ott
  FROM UNNEST(GENERATE_DATE_ARRAY((SELECT MIN(event_dt) FROM `cio-datahub-enterprise-pr-183a.ent_usage_unrated_ott.bq_ott_app_event`), CURRENT_DATE(), INTERVAL 1 WEEK)) as dt 
),

-- Data is collected weekly and will go from Monday -> Sunday inclusive
ts_index AS (
  SELECT
    ROW_NUMBER() OVER(ORDER BY dt ASC) as ts_index,
    * EXCEPT(dt), 
  FROM time_index_stage
  WHERE 
    -- First Monday Date has to be after earliest date in table
    start_dt >= (SELECT MIN(dt) FROM time_index_stage) 
    AND
    -- Last Sunday has to be before last date in app usage, as GENERATE_DATE_ARRAY() won't generate dates past CURRENT_DATE()
    end_dt <= (SELECT MAX(event_dt) FROM `cio-datahub-enterprise-pr-183a.ent_usage_unrated_ott.bq_ott_app_event`) 
),

distinct_imsi AS (
  SELECT DISTINCT imsi_num 
  FROM `cio-datahub-enterprise-pr-183a.ent_usage_unrated_ott.bq_ott_app_event`
  --WHERE imsi_num = '302780002410460' -- remove filter later
),

distinct_categories AS (
  SELECT 
    tier_1,
    tier_2
  FROM `cto-wln-sa-data-pr-bb5283.app_cat_map.bq_app_cat_mapping_latest_view`
  GROUP BY tier_1,tier_2
),

ts_index_imsi AS (
  SELECT 
    A.*,
    B.imsi_num,
    C.tier_1,
    C.tier_2
  FROM ts_index A
  CROSS JOIN distinct_imsi B
  CROSS JOIN distinct_categories C
  
),

app_usage AS (
  SELECT
    A.event_dt,
    A.imsi_num,
    B.tier_1,
    B.tier_2,
    ROUND(SUM(SAFE_DIVIDE(A.ul_volume_qty + A.dl_volume_qty, 1000000.0)), 3) AS usage_volume_MB
  FROM `cio-datahub-enterprise-pr-183a.ent_usage_unrated_ott.bq_ott_app_event` A
  LEFT JOIN `cto-wln-sa-data-pr-bb5283.app_cat_map.bq_app_cat_mapping_latest_view` B 
  ON A.app_nm = B.app_nm
  GROUP BY A.imsi_num, A.event_dt, B.tier_1, B.tier_2
),


app_usage_weekly AS (
  SELECT
    A.*,
    IFNULL(B.usage_volume_MB, 0) AS usage_volume_MB
  FROM ts_index_imsi A 
  LEFT JOIN app_usage B 
  ON
    B.event_dt >= A.start_dt AND B.event_dt <= A.end_dt
    AND A.imsi_num = B.imsi_num
    AND A.tier_1 = B.tier_1
    AND A.tier_2 = B.tier_2 
)

SELECT 
  * 
FROM app_usage_weekly
ORDER BY imsi_num, ts_index, tier_1




