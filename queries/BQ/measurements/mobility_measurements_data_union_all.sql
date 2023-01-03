
WITH mobility_measurements AS (
  SELECT * 
  FROM `cto-wln-sa-data-pr-bb5283.measurements_data.epdmadm_aax_source_2022_ref`
  UNION ALL 

  SELECT * 
  FROM `cto-wln-sa-data-pr-bb5283.measurements_data.epdmadm_dup_multi_new_ref`

  UNION ALL 

  SELECT * 
  FROM `cto-wln-sa-data-pr-bb5283.measurements_data.epdmadm_firefly_campaign_launch_ref`
  UNION ALL 

  SELECT * 
  FROM `cto-wln-sa-data-pr-bb5283.measurements_data.epdmadm_telus_renewal_2022_ref`
  UNION ALL 

  SELECT * 
  FROM `cto-wln-sa-data-pr-bb5283.measurements_data.epdmadm_telus_roaming_2019_ref`
  
),

-- SELECT 
--   CAMPAIGN_IN_HOME_DATE,
--   SUM(DELIVERED) AS TOTAL_DELIVERED_MSGS,
--   SUM(OPENED) AS TOTAL_OPENED_MSGS,
--   SUM(CLICKTHROUGH) AS TOTAL_CLICKTHROUGH_MSGS,
--   SUM(UNSUBSCRIBE) AS TOTAL_UNSUBSCRIBE_EVENTS,
--   SUM(SMS_OPT_OUT) AS TOTAL_SMS_OPT_OUT_EVENTS,
--   SUM(CONVERSION) AS TOTAL_CONVERSIONS
-- FROM mobility_measurements
-- GROUP BY 1

agg_table AS (
  SELECT
    ban, 
    SUBSCRIBER_NO,
    SUM(CONVERSION) AS TOTAL_CONVERSIONS
  FROM mobility_measurements
  GROUP BY 1, 2
)


SELECT
  TOTAL_CONVERSIONS,
  COUNT(*)
FROM agg_table
GROUP BY 1 