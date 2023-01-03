WITH sum_usage AS (
  SELECT 
    BAN,
    MSISDN,
    imsi_id,
    trip_first_date,
    MAX(labels) AS labels,
    SUM(usage_volume_MB_news_weather) AS usage_volume_MB_news_weather,
    SUM(usage_volume_MB_smarthome_security) AS usage_volume_MB_smarthome_security,
    SUM(usage_volume_MB_communication_messaging) AS usage_volume_MB_communication_messaging,
    SUM(usage_volume_MB_travel_trips) AS usage_volume_MB_travel_trips,
    SUM(usage_volume_MB_communication_calling) AS usage_volume_MB_communication_calling,
    SUM(usage_volume_MB_smarthome_automation) AS usage_volume_MB_smarthome_automation,
    SUM(usage_volume_MB_travel_lodging) AS usage_volume_MB_travel_lodging,
    SUM(usage_volume_MB_news_generic) AS usage_volume_MB_news_generic,
    SUM(usage_volume_MB_shopping_miscellaneous) AS usage_volume_MB_shopping_miscellaneous,
    SUM(usage_volume_MB_shopping_food) AS usage_volume_MB_shopping_food,
    SUM(usage_volume_MB_travel_flights) AS usage_volume_MB_travel_flights,
    SUM(usage_volume_MB_travel_airport) AS usage_volume_MB_travel_airport,
    SUM(usage_volume_MB_shopping_fashion) AS usage_volume_MB_shopping_fashion,
    SUM(usage_volume_MB_travel_ridehailing) AS usage_volume_MB_travel_ridehailing,
    SUM(usage_volume_MB_travel_fleetmanagement) AS usage_volume_MB_travel_fleetmanagement,
    SUM(usage_volume_MB_communication_talking) AS usage_volume_MB_communication_talking,
    SUM(usage_volume_MB_news_politics) AS usage_volume_MB_news_politics,
    SUM(usage_volume_MB_news_finance) AS usage_volume_MB_news_finance,
    SUM(usage_volume_MB_travel_travel) AS usage_volume_MB_travel_travel,
    SUM(usage_volume_MB_travel_navigation) AS usage_volume_MB_travel_navigation,
    SUM(usage_volume_MB_travel_smartcar) AS usage_volume_MB_travel_smartcar,
    SUM(usage_volume_MB_communication_socialmedia) AS usage_volume_MB_communication_socialmedia,
    SUM(usage_volume_MB_communication_miscellaneous) AS usage_volume_MB_communication_miscellaneous
  FROM `roaming-pr-66a1b0.pre_trip_modelling_features_v2.cls_pretrip_dataset_ts_random_trips_aug_sept22` 
  GROUP BY BAN, MSISDN, imsi_id, trip_first_date
),

customers_with_zero_usage AS (
  SELECT 
    COUNT(*)
  FROM sum_usage
  WHERE 
    (usage_volume_MB_news_weather + usage_volume_MB_smarthome_security + usage_volume_MB_communication_messaging + usage_volume_MB_travel_trips + usage_volume_MB_communication_calling + usage_volume_MB_smarthome_automation + usage_volume_MB_travel_lodging + usage_volume_MB_news_generic + usage_volume_MB_shopping_miscellaneous + usage_volume_MB_shopping_food + usage_volume_MB_travel_flights + usage_volume_MB_travel_airport + usage_volume_MB_shopping_fashion + usage_volume_MB_travel_ridehailing + usage_volume_MB_travel_fleetmanagement + usage_volume_MB_communication_talking + usage_volume_MB_news_politics + usage_volume_MB_news_finance + usage_volume_MB_travel_travel + usage_volume_MB_travel_navigation + usage_volume_MB_travel_smartcar + usage_volume_MB_communication_socialmedia + usage_volume_MB_communication_miscellaneous) = 0
),

customers_with_low_msf AS (
  SELECT 
    
    CASE WHEN B.pp_recur_chrg_amt >= 50 THEN 'HIGH MSF'
    ELSE 'LOW MSF'
    END AS msf,
    A.labels,
    COUNT(*)
  FROM sum_usage A 
  INNER JOIN `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` B 
  ON A.MSISDN = B.pi_prod_instnc_resrc_str AND A.BAN = B.bacct_bus_bacct_num
  WHERE 
    B.prod_instnc_ts = (SELECT MAX(prod_instnc_ts) FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht`)
    --AND B.pp_recur_chrg_amt >= 50
  GROUP BY 1, 2
  ORDER BY 1
)

SELECT * FROM customers_with_low_msf