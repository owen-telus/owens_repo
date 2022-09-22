-- This returns the result in python as (num_features, time_Steps)
-- Want (time_steps, num_features)
-- SELECT
--   bap_bus_bacct_num,
--   subscriber_num,
--   imsi_id,
--   trip_first_date,
  
--   ARRAY_AGG(IFNULL(usage_volume_MB_news_generic, 0) ORDER BY ts_index ASC) AS usage_volume_MB_news_generic
-- FROM
--   `roaming-pr-66a1b0.roaming_pr.bq_v2_pre_trip_predict_ts` 

-- WHERE
--   subscriber_num IN ('4037154227', '6137954079')

-- GROUP BY
--   bap_bus_bacct_num,
--   subscriber_num,
--   imsi_id,
--   trip_first_date


-- to get (time_steps, num_features) : first get all columns into an array and then pivot ts_index 
WITH array_stage AS (
SELECT 
  bap_bus_bacct_num,
  subscriber_num,
  imsi_id,
  trip_first_date,
  ts_index,
  array[IFNULL(usage_volume_MB_news_finance, 0),
IFNULL(usage_volume_MB_news_generic, 0),
IFNULL(usage_volume_MB_news_weather, 0),
IFNULL(usage_volume_MB_news_politics, 0),
IFNULL(usage_volume_MB_travel_trips, 0),
IFNULL(usage_volume_MB_travel_travel, 0),
IFNULL(usage_volume_MB_travel_airport, 0),
IFNULL(usage_volume_MB_travel_flights, 0),
IFNULL(usage_volume_MB_travel_lodging, 0),
IFNULL(usage_volume_MB_travel_smartcar, 0),
IFNULL(usage_volume_MB_travel_navigation, 0),
IFNULL(usage_volume_MB_travel_ridehailing, 0),
IFNULL(usage_volume_MB_travel_fleetmanagement, 0),
IFNULL(usage_volume_MB_shopping_food, 0),
IFNULL(usage_volume_MB_shopping_fashion, 0),
IFNULL(usage_volume_MB_shopping_miscellaneous, 0),
IFNULL(usage_volume_MB_smarthome_security, 0),
IFNULL(usage_volume_MB_smarthome_automation, 0),
IFNULL(usage_volume_MB_communication_calling, 0),
IFNULL(usage_volume_MB_communication_talking, 0),
IFNULL(usage_volume_MB_communication_messaging, 0),
IFNULL(usage_volume_MB_communication_socialmedia, 0),
IFNULL(usage_volume_MB_communication_miscellaneous, 0),
IFNULL(click_count, 0),
IFNULL(chatbot_cnt, 0)] as array_
FROM  `roaming-pr-66a1b0.roaming_pr.bq_v2_pre_trip_predict_ts` 

WHERE
  subscriber_num IN ('4037154227')
ORDER BY imsi_id,trip_first_date,ts_index
)

SELECT 
  *
FROM array_stage
PIVOT(
  ANY_VALUE(array_) AS timestep
  FOR ts_index IN (1,2,3,4,5,6,7,8)
)