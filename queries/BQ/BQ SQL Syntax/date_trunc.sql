-- Get the date of every Monday since the earliest date in OTT Table (2022-06-18)

-- SELECT 
--   MIN(event_dt) 
-- FROM `cio-datahub-enterprise-pr-183a.ent_usage_unrated_ott.bq_ott_app_event`
-- CREATE OR REPLACE TABLE `cto-wln-sa-data-pr-bb5283.temp_workspace.date_index` AS 
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
  FROM UNNEST(GENERATE_DATE_ARRAY(DATE('2022-06-19'), CURRENT_DATE(), INTERVAL 1 WEEK)) as dt 
),

-- Data is collected weekly and will go from Monday -> Sunday inclusive
time_index AS (
  SELECT
    ROW_NUMBER() OVER(ORDER BY dt ASC) as ts_index,
    * EXCEPT(dt), 
  FROM time_index_stage
  WHERE 
    -- First Monday Date has to be after earliest date in table
    start_dt >= (SELECT MIN(dt) FROM time_index_stage) 
    AND
    -- Last Sunday has to be before last date in app usage, as GENERATE_DATE_ARRAY() won't generate dates past CURRENT_DATE()
    --end_dt <= (SELECT MAX(event_dt) FROM `cio-datahub-enterprise-pr-183a.ent_usage_unrated_ott.bq_ott_app_event`) 
    end_dt < '2022-09-02'
),

time_index_stage_append AS (
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
  FROM UNNEST(GENERATE_DATE_ARRAY((SELECT MAX(start_dt) FROM time_index), CURRENT_DATE(), INTERVAL 1 WEEK)) as dt 
),

time_index_append AS (
  SELECT
    (SELECT MAX(ts_index) FROM time_index) + ROW_NUMBER() OVER(ORDER BY dt ASC) as ts_index,
    * EXCEPT(dt), 
  FROM time_index_stage_append
  WHERE 
    -- First Monday Date has to be after earliest date in table
    start_dt > (SELECT MAX(start_dt) FROM time_index) 
    AND
    -- Last Sunday has to be before last date in app usage, as GENERATE_DATE_ARRAY() won't generate dates past CURRENT_DATE()
    -- This below will be my filter
    end_dt <= DATE('2022-09-15')  --(SELECT MAX(event_dt) FROM `cio-datahub-enterprise-pr-183a.ent_usage_unrated_ott.bq_ott_app_event`) 
    
)

SELECT 
  *
  
FROM time_index_append




-- SELECT
--   *
-- FROM time_index
-- UNION ALL
-- SELECT
--   MAX(ts_index) + 1 AS ts_index,
--   DATE_ADD(MAX(start_dt), INTERVAL 1 WEEK) AS start_dt, -- From previous week
--   -- Only append when: end_dt <= last date in ott app table -> This will be a condition in the MERGE INSERT
--   -- MERGE INSERT will be on the dates 
--   DATE_ADD(MAX(end_dt), INTERVAL 1 WEEK) AS end_dt 
-- FROM time_index
