-- This table creates the initial table: cto-wln-sa-data-pr-bb5283.customer_personas_features.imsi_app_usage_trends_weekly_ts

-- All I need to do is create this initial table, and then create stored procedure for weekly updates
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
    --end_dt < '2022-09-02'
),

distinct_imsi AS (
  SELECT DISTINCT imsi_num 
  FROM `cio-datahub-enterprise-pr-183a.ent_usage_unrated_ott.bq_ott_app_event`
  --WHERE imsi_num = '302780002410460' -- remove filter later
),

ts_index_imsi AS (
  SELECT 
    A.*,
    B.imsi_num
  FROM ts_index A
  CROSS JOIN distinct_imsi B

),

app_usage AS (
  SELECT
    A.event_dt,
    A.imsi_num,
    CONCAT(REGEXP_REPLACE(TRIM(LOWER(B.tier_1)),r'[^a-zA-Z]',''), '_', REGEXP_REPLACE(TRIM(LOWER(B.tier_2)),r'[^a-zA-Z]','')) AS category,
    ROUND(SUM(SAFE_DIVIDE(A.ul_volume_qty + A.dl_volume_qty, 1000000.0)), 3) AS usage_volume_MB
  FROM `cio-datahub-enterprise-pr-183a.ent_usage_unrated_ott.bq_ott_app_event` A
  LEFT JOIN `cto-wln-sa-data-pr-bb5283.app_cat_map.bq_app_cat_mapping_latest_view` B 
  ON A.app_nm = B.app_nm
  --WHERE 
    -- A.imsi_num = '302780002410460'
    --AND A.event_dt >= '2022-09-10' -- Remove filter when doing weekly agg
  GROUP BY A.imsi_num, A.event_dt, CONCAT(REGEXP_REPLACE(TRIM(LOWER(B.tier_1)),r'[^a-zA-Z]',''), '_', REGEXP_REPLACE(TRIM(LOWER(B.tier_2)),r'[^a-zA-Z]',''))
),

app_usage_pivot AS (
  SELECT
    *
  FROM app_usage
  PIVOT(
      MAX(usage_volume_MB) AS usage_volume_MB
      FOR category in ("communication_talking", "travel_trips", "telecom_wirelesswirelinesmarthome", "gaming_kids", "productivity_webbrowser", "finance_investing", "news_politics", "lifestyle_mentalhealth", "news_finance", "network_protocol", "travel_airport", "telecom_pricecomparison", "telecom_smarthome", "shopping_food", "finance_utilities", "pets_cats", "lifestyle_healthcare", "network_security", "gaming_moderate", "smarthome_security", "network_other", "entertainment_radio", "entertainment_sports", "entertainment_live", "productivity_softwaredevelopment", "agriculture_farmmanagement", "telecom_wirelesswireline", "travel_fleetmanagement", "travel_navigation", "pets_other", "productivity_education", "smarthome_automation", "realestate_landlord", "entertainment_reading", "os_appstore", "entertainment_news", "shopping_miscellaneous", "travel_lodging", "productivity_filetransfer", "finance_banking", "entertainment_tvandmovies", "travel_ridehailing", "travel_flights", "telecom_telus", "communication_messaging", "realestate_moving", "productivity_other", "pets_dogs", "entertainment_kids", "entertainment_music", "productivity_email", "utility_security", "utility_devicecare", "lifestyle_kids", "news_generic", "realestate_buysellrent", "lifestyle_relationships", "shopping_fashion", "travel_smartcar", "pets_petssupplies", "productivity_editingsoftware", "telecom_others", "telecom_benchmark", "productivity_storage", "news_weather", "travel_travel", "finance_business", "finance_moneytransfer", "lifestyle_fitness", "communication_calling", "lifestyle_identification", "telecom_wireline", "telecom_wireless", "agriculture_farmassociations", "os_update", "productivity_professional", "os_other", "gaming_cloud", "gaming_casual", "communication_miscellaneous", "communication_socialmedia", "entertainment_miscellaneous", "realestate_projectmanagement", "finance_paymentapps")
  )
),

app_usage_weekly_stage AS (
  SELECT
    A.*,
    B.* EXCEPT(imsi_num)
  FROM ts_index_imsi A 
  LEFT JOIN app_usage_pivot B 
  ON
    B.event_dt >= A.start_dt AND B.event_dt <= A.end_dt
    AND A.imsi_num = B.imsi_num
),


app_usage_weekly AS (
  SELECT 
    imsi_num,
    ts_index,
    start_dt,
    end_dt,
        
    IFNULL(ROUND(SUM(usage_volume_MB_entertainment_sports),2), 0) AS usage_volume_MB_entertainment_sports, 
    IFNULL(ROUND(SUM(usage_volume_MB_realestate_landlord),2), 0) AS usage_volume_MB_realestate_landlord, 
    IFNULL(ROUND(SUM(usage_volume_MB_agriculture_farmmanagement),2), 0) AS usage_volume_MB_agriculture_farmmanagement, 
    IFNULL(ROUND(SUM(usage_volume_MB_productivity_education),2), 0) AS usage_volume_MB_productivity_education, 
    IFNULL(ROUND(SUM(usage_volume_MB_telecom_telus),2), 0) AS usage_volume_MB_telecom_telus, 
    IFNULL(ROUND(SUM(usage_volume_MB_realestate_buysellrent),2), 0) AS usage_volume_MB_realestate_buysellrent, 
    IFNULL(ROUND(SUM(usage_volume_MB_travel_trips),2), 0) AS usage_volume_MB_travel_trips, 
    IFNULL(ROUND(SUM(usage_volume_MB_finance_banking),2), 0) AS usage_volume_MB_finance_banking, 
    IFNULL(ROUND(SUM(usage_volume_MB_pets_dogs),2), 0) AS usage_volume_MB_pets_dogs, 
    IFNULL(ROUND(SUM(usage_volume_MB_lifestyle_mentalhealth),2), 0) AS usage_volume_MB_lifestyle_mentalhealth, 
    IFNULL(ROUND(SUM(usage_volume_MB_os_appstore),2), 0) AS usage_volume_MB_os_appstore, 
    IFNULL(ROUND(SUM(usage_volume_MB_news_generic),2), 0) AS usage_volume_MB_news_generic, 
    IFNULL(ROUND(SUM(usage_volume_MB_entertainment_music),2), 0) AS usage_volume_MB_entertainment_music, 
    IFNULL(ROUND(SUM(usage_volume_MB_communication_miscellaneous),2), 0) AS usage_volume_MB_communication_miscellaneous, 
    IFNULL(ROUND(SUM(usage_volume_MB_gaming_moderate),2), 0) AS usage_volume_MB_gaming_moderate, 
    IFNULL(ROUND(SUM(usage_volume_MB_utility_security),2), 0) AS usage_volume_MB_utility_security, 
    IFNULL(ROUND(SUM(usage_volume_MB_communication_socialmedia),2), 0) AS usage_volume_MB_communication_socialmedia, 
    IFNULL(ROUND(SUM(usage_volume_MB_productivity_other),2), 0) AS usage_volume_MB_productivity_other, 
    IFNULL(ROUND(SUM(usage_volume_MB_shopping_fashion),2), 0) AS usage_volume_MB_shopping_fashion, 
    IFNULL(ROUND(SUM(usage_volume_MB_productivity_webbrowser),2), 0) AS usage_volume_MB_productivity_webbrowser, 
    IFNULL(ROUND(SUM(usage_volume_MB_finance_utilities),2), 0) AS usage_volume_MB_finance_utilities, 
    IFNULL(ROUND(SUM(usage_volume_MB_travel_travel),2), 0) AS usage_volume_MB_travel_travel, 
    IFNULL(ROUND(SUM(usage_volume_MB_lifestyle_fitness),2), 0) AS usage_volume_MB_lifestyle_fitness, 
    IFNULL(ROUND(SUM(usage_volume_MB_finance_business),2), 0) AS usage_volume_MB_finance_business, 
    IFNULL(ROUND(SUM(usage_volume_MB_lifestyle_relationships),2), 0) AS usage_volume_MB_lifestyle_relationships, 
    IFNULL(ROUND(SUM(usage_volume_MB_telecom_wireline),2), 0) AS usage_volume_MB_telecom_wireline, 
    IFNULL(ROUND(SUM(usage_volume_MB_travel_lodging),2), 0) AS usage_volume_MB_travel_lodging, 
    IFNULL(ROUND(SUM(usage_volume_MB_productivity_editingsoftware),2), 0) AS usage_volume_MB_productivity_editingsoftware, 
    IFNULL(ROUND(SUM(usage_volume_MB_pets_petssupplies),2), 0) AS usage_volume_MB_pets_petssupplies, 
    IFNULL(ROUND(SUM(usage_volume_MB_travel_ridehailing),2), 0) AS usage_volume_MB_travel_ridehailing, 
    IFNULL(ROUND(SUM(usage_volume_MB_agriculture_farmassociations),2), 0) AS usage_volume_MB_agriculture_farmassociations, 
    IFNULL(ROUND(SUM(usage_volume_MB_productivity_professional),2), 0) AS usage_volume_MB_productivity_professional, 
    IFNULL(ROUND(SUM(usage_volume_MB_smarthome_automation),2), 0) AS usage_volume_MB_smarthome_automation, 
    IFNULL(ROUND(SUM(usage_volume_MB_lifestyle_healthcare),2), 0) AS usage_volume_MB_lifestyle_healthcare, 
    IFNULL(ROUND(SUM(usage_volume_MB_entertainment_tvandmovies),2), 0) AS usage_volume_MB_entertainment_tvandmovies, 
    IFNULL(ROUND(SUM(usage_volume_MB_travel_flights),2), 0) AS usage_volume_MB_travel_flights, 
    IFNULL(ROUND(SUM(usage_volume_MB_gaming_casual),2), 0) AS usage_volume_MB_gaming_casual, 
    IFNULL(ROUND(SUM(usage_volume_MB_finance_paymentapps),2), 0) AS usage_volume_MB_finance_paymentapps, 
    IFNULL(ROUND(SUM(usage_volume_MB_gaming_kids),2), 0) AS usage_volume_MB_gaming_kids, 
    IFNULL(ROUND(SUM(usage_volume_MB_travel_navigation),2), 0) AS usage_volume_MB_travel_navigation, 
    IFNULL(ROUND(SUM(usage_volume_MB_utility_devicecare),2), 0) AS usage_volume_MB_utility_devicecare, 
    IFNULL(ROUND(SUM(usage_volume_MB_telecom_wirelesswireline),2), 0) AS usage_volume_MB_telecom_wirelesswireline, 
    IFNULL(ROUND(SUM(usage_volume_MB_entertainment_news),2), 0) AS usage_volume_MB_entertainment_news, 
    IFNULL(ROUND(SUM(usage_volume_MB_telecom_others),2), 0) AS usage_volume_MB_telecom_others, 
    IFNULL(ROUND(SUM(usage_volume_MB_news_weather),2), 0) AS usage_volume_MB_news_weather, 
    IFNULL(ROUND(SUM(usage_volume_MB_entertainment_radio),2), 0) AS usage_volume_MB_entertainment_radio, 
    IFNULL(ROUND(SUM(usage_volume_MB_travel_airport),2), 0) AS usage_volume_MB_travel_airport, 
    IFNULL(ROUND(SUM(usage_volume_MB_entertainment_live),2), 0) AS usage_volume_MB_entertainment_live, 
    IFNULL(ROUND(SUM(usage_volume_MB_telecom_pricecomparison),2), 0) AS usage_volume_MB_telecom_pricecomparison, 
    IFNULL(ROUND(SUM(usage_volume_MB_pets_other),2), 0) AS usage_volume_MB_pets_other, 
    IFNULL(ROUND(SUM(usage_volume_MB_lifestyle_identification),2), 0) AS usage_volume_MB_lifestyle_identification, 
    IFNULL(ROUND(SUM(usage_volume_MB_realestate_projectmanagement),2), 0) AS usage_volume_MB_realestate_projectmanagement, 
    IFNULL(ROUND(SUM(usage_volume_MB_communication_talking),2), 0) AS usage_volume_MB_communication_talking, 
    IFNULL(ROUND(SUM(usage_volume_MB_network_security),2), 0) AS usage_volume_MB_network_security, 
    IFNULL(ROUND(SUM(usage_volume_MB_realestate_moving),2), 0) AS usage_volume_MB_realestate_moving, 
    IFNULL(ROUND(SUM(usage_volume_MB_productivity_email),2), 0) AS usage_volume_MB_productivity_email, 
    IFNULL(ROUND(SUM(usage_volume_MB_telecom_wireless),2), 0) AS usage_volume_MB_telecom_wireless, 
    IFNULL(ROUND(SUM(usage_volume_MB_travel_fleetmanagement),2), 0) AS usage_volume_MB_travel_fleetmanagement, 
    IFNULL(ROUND(SUM(usage_volume_MB_communication_messaging),2), 0) AS usage_volume_MB_communication_messaging, 
    IFNULL(ROUND(SUM(usage_volume_MB_gaming_cloud),2), 0) AS usage_volume_MB_gaming_cloud, 
    IFNULL(ROUND(SUM(usage_volume_MB_os_other),2), 0) AS usage_volume_MB_os_other, 
    IFNULL(ROUND(SUM(usage_volume_MB_entertainment_reading),2), 0) AS usage_volume_MB_entertainment_reading, 
    IFNULL(ROUND(SUM(usage_volume_MB_finance_moneytransfer),2), 0) AS usage_volume_MB_finance_moneytransfer, 
    IFNULL(ROUND(SUM(usage_volume_MB_telecom_wirelesswirelinesmarthome),2), 0) AS usage_volume_MB_telecom_wirelesswirelinesmarthome, 
    IFNULL(ROUND(SUM(usage_volume_MB_news_politics),2), 0) AS usage_volume_MB_news_politics, 
    IFNULL(ROUND(SUM(usage_volume_MB_telecom_smarthome),2), 0) AS usage_volume_MB_telecom_smarthome, 
    IFNULL(ROUND(SUM(usage_volume_MB_productivity_softwaredevelopment),2), 0) AS usage_volume_MB_productivity_softwaredevelopment, 
    IFNULL(ROUND(SUM(usage_volume_MB_lifestyle_kids),2), 0) AS usage_volume_MB_lifestyle_kids, 
    IFNULL(ROUND(SUM(usage_volume_MB_telecom_benchmark),2), 0) AS usage_volume_MB_telecom_benchmark, 
    IFNULL(ROUND(SUM(usage_volume_MB_os_update),2), 0) AS usage_volume_MB_os_update, 
    IFNULL(ROUND(SUM(usage_volume_MB_communication_calling),2), 0) AS usage_volume_MB_communication_calling, 
    IFNULL(ROUND(SUM(usage_volume_MB_entertainment_miscellaneous),2), 0) AS usage_volume_MB_entertainment_miscellaneous, 
    IFNULL(ROUND(SUM(usage_volume_MB_network_protocol),2), 0) AS usage_volume_MB_network_protocol, 
    IFNULL(ROUND(SUM(usage_volume_MB_smarthome_security),2), 0) AS usage_volume_MB_smarthome_security, 
    IFNULL(ROUND(SUM(usage_volume_MB_pets_cats),2), 0) AS usage_volume_MB_pets_cats, 
    IFNULL(ROUND(SUM(usage_volume_MB_news_finance),2), 0) AS usage_volume_MB_news_finance, 
    IFNULL(ROUND(SUM(usage_volume_MB_productivity_filetransfer),2), 0) AS usage_volume_MB_productivity_filetransfer, 
    IFNULL(ROUND(SUM(usage_volume_MB_entertainment_kids),2), 0) AS usage_volume_MB_entertainment_kids, 
    IFNULL(ROUND(SUM(usage_volume_MB_finance_investing),2), 0) AS usage_volume_MB_finance_investing, 
    IFNULL(ROUND(SUM(usage_volume_MB_shopping_food),2), 0) AS usage_volume_MB_shopping_food, 
    IFNULL(ROUND(SUM(usage_volume_MB_shopping_miscellaneous),2), 0) AS usage_volume_MB_shopping_miscellaneous, 
    IFNULL(ROUND(SUM(usage_volume_MB_network_other),2), 0) AS usage_volume_MB_network_other, 
    IFNULL(ROUND(SUM(usage_volume_MB_productivity_storage),2), 0) AS usage_volume_MB_productivity_storage, 
    IFNULL(ROUND(SUM(usage_volume_MB_travel_smartcar),2), 0) AS usage_volume_MB_travel_smartcar

  FROM app_usage_weekly_stage
  GROUP BY imsi_num, ts_index, start_dt, end_dt
  
)

SELECT * 
FROM app_usage_weekly 
ORDER BY imsi_num, ts_index




