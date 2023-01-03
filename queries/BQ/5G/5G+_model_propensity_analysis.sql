-- -- Non TS

-- SELECT
--   A.* EXCEPT(propensity),
--   A.propensity AS lgbm_propensity_v1,
--   B.propensity AS rf_propensity_v1,
--   C.propensity AS xgb_propensity_v1,
--   D.propensity AS lgbm_propensity_v2,
--   E.propensity AS rf_propensity_v2,
--   F.propensity AS gbc_propensity_v2

-- FROM `cto-wln-sa-data-pr-bb5283.5G_speed_tiers.lgbm_predictions_v1` A
-- INNER JOIN `cto-wln-sa-data-pr-bb5283.5G_speed_tiers.rf_predictions_v1` B 
-- ON A.BAN = B.BAN AND A.MSISDN = B.MSISDN AND A.IMSI = B.IMSI
-- INNER JOIN `cto-wln-sa-data-pr-bb5283.5G_speed_tiers.xgb_predictions_v1` C 
-- ON A.BAN = C.BAN AND A.MSISDN = C.MSISDN AND A.IMSI = C.IMSI
-- INNER JOIN `cto-wln-sa-data-pr-bb5283.5G_speed_tiers.lgbm_predictions_v2` D 
-- ON A.BAN = D.BAN AND A.MSISDN = D.MSISDN AND A.IMSI = D.IMSI
-- INNER JOIN `cto-wln-sa-data-pr-bb5283.5G_speed_tiers.rf_predictions_v2` E 
-- ON A.BAN = E.BAN AND A.MSISDN = E.MSISDN AND A.IMSI = E.IMSI
-- INNER JOIN `cto-wln-sa-data-pr-bb5283.5G_speed_tiers.gbc_predictions_v2` F 
-- ON A.BAN = F.BAN AND A.MSISDN = F.MSISDN AND A.IMSI = F.IMSI


-- TS 

WITH ts_app_usage AS (
  SELECT
  imsi_num,
  ts_index,
  start_dt,
  usage_volume_MB_communication_calling + usage_volume_MB_communication_messaging + usage_volume_MB_communication_miscellaneous + usage_volume_MB_communication_socialmedia + usage_volume_MB_communication_talking AS usage_volume_MB_communication,
usage_volume_MB_entertainment_kids + usage_volume_MB_entertainment_live + usage_volume_MB_entertainment_miscellaneous + usage_volume_MB_entertainment_music + usage_volume_MB_entertainment_news + usage_volume_MB_entertainment_radio + usage_volume_MB_entertainment_reading + usage_volume_MB_entertainment_sports + usage_volume_MB_entertainment_tvandmovies AS usage_volume_MB_entertainment,
usage_volume_MB_finance_banking + usage_volume_MB_finance_business + usage_volume_MB_finance_investing + usage_volume_MB_finance_moneytransfer + usage_volume_MB_finance_paymentapps + usage_volume_MB_finance_utilities + usage_volume_MB_news_finance AS usage_volume_MB_finance,
usage_volume_MB_gaming_casual + usage_volume_MB_gaming_cloud + usage_volume_MB_gaming_kids + usage_volume_MB_gaming_moderate AS usage_volume_MB_gaming,
usage_volume_MB_lifestyle_fitness + usage_volume_MB_lifestyle_healthcare + usage_volume_MB_lifestyle_identification + usage_volume_MB_lifestyle_kids + usage_volume_MB_lifestyle_mentalhealth + usage_volume_MB_lifestyle_relationships AS usage_volume_MB_lifestyle,
usage_volume_MB_entertainment_news + usage_volume_MB_news_finance + usage_volume_MB_news_generic + usage_volume_MB_news_politics + usage_volume_MB_news_weather AS usage_volume_MB_news,
usage_volume_MB_os_appstore + usage_volume_MB_os_other + usage_volume_MB_os_update AS usage_volume_MB_os,
usage_volume_MB_productivity_editingsoftware + usage_volume_MB_productivity_education + usage_volume_MB_productivity_email + usage_volume_MB_productivity_filetransfer + usage_volume_MB_productivity_other + usage_volume_MB_productivity_professional + usage_volume_MB_productivity_softwaredevelopment + usage_volume_MB_productivity_storage + usage_volume_MB_productivity_webbrowser AS usage_volume_MB_productivity,
usage_volume_MB_shopping_fashion + usage_volume_MB_shopping_food + usage_volume_MB_shopping_miscellaneous AS usage_volume_MB_shopping,
usage_volume_MB_smarthome_automation + usage_volume_MB_smarthome_security + usage_volume_MB_telecom_smarthome + usage_volume_MB_telecom_wirelesswirelinesmarthome AS usage_volume_MB_smarthome,
usage_volume_MB_telecom_benchmark + usage_volume_MB_telecom_others + usage_volume_MB_telecom_pricecomparison + usage_volume_MB_telecom_smarthome + usage_volume_MB_telecom_telus + usage_volume_MB_telecom_wireless + usage_volume_MB_telecom_wirelesswireline + usage_volume_MB_telecom_wirelesswirelinesmarthome + usage_volume_MB_telecom_wireline AS usage_volume_MB_telecom,
usage_volume_MB_travel_airport + usage_volume_MB_travel_fleetmanagement + usage_volume_MB_travel_flights + usage_volume_MB_travel_lodging + usage_volume_MB_travel_navigation + usage_volume_MB_travel_ridehailing + usage_volume_MB_travel_smartcar + usage_volume_MB_travel_travel + usage_volume_MB_travel_trips AS usage_volume_MB_travel,
usage_volume_MB_realestate_buysellrent + usage_volume_MB_realestate_landlord + usage_volume_MB_realestate_moving + usage_volume_MB_realestate_projectmanagement AS usage_volume_MB_realestate,
usage_volume_MB_pets_cats + usage_volume_MB_pets_dogs + usage_volume_MB_pets_other + usage_volume_MB_pets_petssupplies AS usage_volume_MB_pets,
FROM `cto-wln-sa-data-pr-bb5283.customer_personas_features.imsi_app_usage_trends_weekly_ts`

-- Most recent 4 weeks of data 
WHERE ts_index > (SELECT MAX(ts_index) -4 FROM `cto-wln-sa-data-pr-bb5283.customer_personas_features.imsi_app_usage_trends_weekly_ts`)


)

SELECT
  A.IMSI,
  A.BAN,
  A.MSISDN,
  A.plan_type,
  A.propensity AS lgbm_propensity_v1,
  B.propensity AS rf_propensity_v1,
  C.propensity AS xgb_propensity_v1,
  D.propensity AS lgbm_propensity_v2,
  E.propensity AS rf_propensity_v2,
  F.propensity AS gbc_propensity_v2,
  G.* EXCEPT(imsi_num)

FROM `cto-wln-sa-data-pr-bb5283.5G_speed_tiers.lgbm_predictions_v1` A
INNER JOIN `cto-wln-sa-data-pr-bb5283.5G_speed_tiers.rf_predictions_v1` B 
ON A.BAN = B.BAN AND A.MSISDN = B.MSISDN AND A.IMSI = B.IMSI
INNER JOIN `cto-wln-sa-data-pr-bb5283.5G_speed_tiers.xgb_predictions_v1` C 
ON A.BAN = C.BAN AND A.MSISDN = C.MSISDN AND A.IMSI = C.IMSI
INNER JOIN `cto-wln-sa-data-pr-bb5283.5G_speed_tiers.lgbm_predictions_v2` D 
ON A.BAN = D.BAN AND A.MSISDN = D.MSISDN AND A.IMSI = D.IMSI
INNER JOIN `cto-wln-sa-data-pr-bb5283.5G_speed_tiers.rf_predictions_v2` E 
ON A.BAN = E.BAN AND A.MSISDN = E.MSISDN AND A.IMSI = E.IMSI
INNER JOIN `cto-wln-sa-data-pr-bb5283.5G_speed_tiers.gbc_predictions_v2` F 
ON A.BAN = F.BAN AND A.MSISDN = F.MSISDN AND A.IMSI = F.IMSI
INNER JOIN ts_app_usage G 
ON A.IMSI = G.imsi_num
