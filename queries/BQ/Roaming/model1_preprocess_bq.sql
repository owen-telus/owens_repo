-- Query to pre_process pre_trip model1 data in SQL instead of Python

-- Sort data by imsi, trip first date, and ts_index first to make query more efficient

WITH sorted_ts_data AS (
      SELECT
        *
      FROM `roaming-pr-66a1b0.roaming_pr.bq_v2_pre_trip_predict_ts`
      ORDER BY imsi_id, trip_first_date, ts_index
    ),

    processed_ts_data AS (
      SELECT
        A.bap_bus_bacct_num,
        A.subscriber_num,
        A.imsi_id,
        A.trip_first_date,
        A.ts_index,

        TRUNC(COS(2*ACOS(-1)*EXTRACT(MONTH from CAST(A.dt as DATE)) / 12), 6) AS dt_cosine_month,
        TRUNC(SIN(2*ACOS(-1)*EXTRACT(MONTH from CAST(A.dt as DATE)) / 12), 6) AS dt_sine_month,
        TRUNC(COS(2*ACOS(-1)*EXTRACT(DAY from CAST(A.dt as DATE)) / 31), 6) AS dt_cosine_day,
        TRUNC(SIN(2*ACOS(-1)*EXTRACT(DAY from CAST(A.dt as DATE)) / 31), 6) AS dt_sine_day,

        SAFE_DIVIDE((IFNULL(A.usage_volume_MB_news_finance, 0) - B.usage_volume_MB_news_finance_mean_coef), SQRT(B.usage_volume_MB_news_finance_var_coef)) AS usage_volume_MB_news_finance,
        SAFE_DIVIDE((IFNULL(A.usage_volume_MB_travel_lodging, 0) - B.usage_volume_MB_travel_lodging_mean_coef), SQRT(B.usage_volume_MB_travel_lodging_var_coef)) AS usage_volume_MB_travel_lodging,
        SAFE_DIVIDE((IFNULL(A.usage_volume_MB_communication_talking, 0) - B.usage_volume_MB_communication_talking_mean_coef), SQRT(B.usage_volume_MB_communication_talking_var_coef)) AS usage_volume_MB_communication_talking,
        SAFE_DIVIDE((IFNULL(A.usage_volume_MB_communication_calling, 0) - B.usage_volume_MB_communication_calling_mean_coef), SQRT(B.usage_volume_MB_communication_calling_var_coef)) AS usage_volume_MB_communication_calling,
        SAFE_DIVIDE((IFNULL(A.usage_volume_MB_travel_fleetmanagement, 0) - B.usage_volume_MB_travel_fleetmanagement_mean_coef), SQRT(B.usage_volume_MB_travel_fleetmanagement_var_coef)) AS usage_volume_MB_travel_fleetmanagement,
        SAFE_DIVIDE((IFNULL(A.usage_volume_MB_travel_trips, 0) - B.usage_volume_MB_travel_trips_mean_coef), SQRT(B.usage_volume_MB_travel_trips_var_coef)) AS usage_volume_MB_travel_trips,
        SAFE_DIVIDE((IFNULL(A.usage_volume_MB_travel_ridehailing, 0) - B.usage_volume_MB_travel_ridehailing_mean_coef), SQRT(B.usage_volume_MB_travel_ridehailing_var_coef)) AS usage_volume_MB_travel_ridehailing,
        SAFE_DIVIDE((IFNULL(A.usage_volume_MB_communication_messaging, 0) - B.usage_volume_MB_communication_messaging_mean_coef), SQRT(B.usage_volume_MB_communication_messaging_var_coef)) AS usage_volume_MB_communication_messaging,
        SAFE_DIVIDE((IFNULL(A.usage_volume_MB_travel_navigation, 0) - B.usage_volume_MB_travel_navigation_mean_coef), SQRT(B.usage_volume_MB_travel_navigation_var_coef)) AS usage_volume_MB_travel_navigation,
        SAFE_DIVIDE((IFNULL(A.usage_volume_MB_shopping_food, 0) - B.usage_volume_MB_shopping_food_mean_coef), SQRT(B.usage_volume_MB_shopping_food_var_coef)) AS usage_volume_MB_shopping_food,
        SAFE_DIVIDE((IFNULL(A.usage_volume_MB_travel_flights, 0) - B.usage_volume_MB_travel_flights_mean_coef), SQRT(B.usage_volume_MB_travel_flights_var_coef)) AS usage_volume_MB_travel_flights,
        SAFE_DIVIDE((IFNULL(A.usage_volume_MB_communication_socialmedia, 0) - B.usage_volume_MB_communication_socialmedia_mean_coef), SQRT(B.usage_volume_MB_communication_socialmedia_var_coef)) AS usage_volume_MB_communication_socialmedia,
        SAFE_DIVIDE((IFNULL(A.usage_volume_MB_smarthome_automation, 0) - B.usage_volume_MB_smarthome_automation_mean_coef), SQRT(B.usage_volume_MB_smarthome_automation_var_coef)) AS usage_volume_MB_smarthome_automation,
        SAFE_DIVIDE((IFNULL(A.usage_volume_MB_travel_travel, 0) - B.usage_volume_MB_travel_travel_mean_coef), SQRT(B.usage_volume_MB_travel_travel_var_coef)) AS usage_volume_MB_travel_travel,
        SAFE_DIVIDE((IFNULL(A.usage_volume_MB_news_weather, 0) - B.usage_volume_MB_news_weather_mean_coef), SQRT(B.usage_volume_MB_news_weather_var_coef)) AS usage_volume_MB_news_weather,
        SAFE_DIVIDE((IFNULL(A.usage_volume_MB_news_politics, 0) - B.usage_volume_MB_news_politics_mean_coef), SQRT(B.usage_volume_MB_news_politics_var_coef)) AS usage_volume_MB_news_politics,
        SAFE_DIVIDE((IFNULL(A.usage_volume_MB_smarthome_security, 0) - B.usage_volume_MB_smarthome_security_mean_coef), SQRT(B.usage_volume_MB_smarthome_security_var_coef)) AS usage_volume_MB_smarthome_security,
        SAFE_DIVIDE((IFNULL(A.usage_volume_MB_news_generic, 0) - B.usage_volume_MB_news_generic_mean_coef), SQRT(B.usage_volume_MB_news_generic_var_coef)) AS usage_volume_MB_news_generic,
        SAFE_DIVIDE((IFNULL(A.usage_volume_MB_shopping_miscellaneous, 0) - B.usage_volume_MB_shopping_miscellaneous_mean_coef), SQRT(B.usage_volume_MB_shopping_miscellaneous_var_coef)) AS usage_volume_MB_shopping_miscellaneous,
        SAFE_DIVIDE((IFNULL(A.usage_volume_MB_communication_miscellaneous, 0) - B.usage_volume_MB_communication_miscellaneous_mean_coef), SQRT(B.usage_volume_MB_communication_miscellaneous_var_coef)) AS usage_volume_MB_communication_miscellaneous,
        SAFE_DIVIDE((IFNULL(A.usage_volume_MB_shopping_fashion, 0) - B.usage_volume_MB_shopping_fashion_mean_coef), SQRT(B.usage_volume_MB_shopping_fashion_var_coef)) AS usage_volume_MB_shopping_fashion,
        SAFE_DIVIDE((IFNULL(A.usage_volume_MB_travel_smartcar, 0) - B.usage_volume_MB_travel_smartcar_mean_coef), SQRT(B.usage_volume_MB_travel_smartcar_var_coef)) AS usage_volume_MB_travel_smartcar,
        SAFE_DIVIDE((IFNULL(A.usage_volume_MB_travel_airport, 0) - B.usage_volume_MB_travel_airport_mean_coef), SQRT(B.usage_volume_MB_travel_airport_var_coef)) AS usage_volume_MB_travel_airport,
        SAFE_DIVIDE((IFNULL(A.click_count, 0) - B.clickstream_cnt_mean_coef), SQRT(B.clickstream_cnt_var_coef)) AS clickstream_cnt,
        SAFE_DIVIDE((IFNULL(A.chatbot_cnt, 0) - B.chatbot_cnt_mean_coef), SQRT(B.chatbot_cnt_var_coef)) AS chatbot_cnt


      FROM sorted_ts_data A, `roaming-pr-66a1b0.roaming_pr.pre_trip_model1_stdscaler_coef` B

    ),

    reshaped_ts_data_stage AS (
      SELECT 
        bap_bus_bacct_num,
        subscriber_num,
        imsi_id,
        trip_first_date,
        ts_index,
        array[
          dt_cosine_month,
          dt_sine_month,
          dt_cosine_day,
          dt_sine_day, 
          usage_volume_MB_news_finance,
          usage_volume_MB_travel_lodging,
          usage_volume_MB_communication_talking,
          usage_volume_MB_communication_calling,
          usage_volume_MB_travel_fleetmanagement,
          usage_volume_MB_travel_trips,
          usage_volume_MB_travel_ridehailing,
          usage_volume_MB_communication_messaging,
          usage_volume_MB_travel_navigation,
          usage_volume_MB_shopping_food,
          usage_volume_MB_travel_flights,
          usage_volume_MB_communication_socialmedia,
          usage_volume_MB_smarthome_automation,
          usage_volume_MB_travel_travel,
          usage_volume_MB_news_weather,
          usage_volume_MB_news_politics,
          usage_volume_MB_smarthome_security,
          usage_volume_MB_news_generic,
          usage_volume_MB_shopping_miscellaneous,
          usage_volume_MB_communication_miscellaneous,
          usage_volume_MB_shopping_fashion,
          usage_volume_MB_travel_smartcar,
          usage_volume_MB_travel_airport,
          clickstream_cnt,
          chatbot_cnt
        ] AS array_
      FROM processed_ts_data
    ),

    reshaped_ts_data AS (
      SELECT * 
      FROM reshaped_ts_data_stage
      PIVOT(
        ANY_VALUE(array_) AS timestep
        FOR ts_index IN (1,2,3,4,5,6,7,8)
      )
    )
    SELECT * FROM reshaped_ts_data