WITH ott_min_date AS (
  SELECT
    MIN(event_dt) AS min_date 
  FROM `cio-datahub-enterprise-pr-183a.ent_usage_unrated_ott.bq_ott_app_event`

),

weekly_ts_index AS (
  SELECT
    dt AS start_date,
    DATE_ADD(dt, INTERVAL 1 WEEK) AS end_date,
    ROW_NUMBER() OVER(ORDER BY dt ASC) as ts_index -- Create numerical index
  FROM UNNEST(GENERATE_DATE_ARRAY(DATE('2022-05-23'), CURRENT_DATE(), INTERVAL 1 WEEK)) as dt
)

SELECT 
* 
FROM weekly_ts_index

