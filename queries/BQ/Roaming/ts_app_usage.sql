-- Create Time Series Dataset for App Usage Based on customers who roamed from pre_trip_labels_final

DECLARE app_usage_categories_pivot_str STRING;

EXECUTE IMMEDIATE """
  CREATE TEMP TABLE app_usage AS
  WITH app_usage_data AS (
    SELECT
      A.event_dt,
      A.imsi_num,
      CONCAT(REGEXP_REPLACE(TRIM(LOWER(B.tier_1)),r'[^a-zA-Z]',''), '_', REGEXP_REPLACE(TRIM(LOWER(B.tier_2)),r'[^a-zA-Z]','')) AS category,
      ROUND(SUM(SAFE_DIVIDE(A.ul_volume_qty + A.dl_volume_qty, 1000000.0)), 3) AS usage_volume_MB
    FROM `cio-datahub-enterprise-pr-183a.ent_usage_unrated_ott.bq_ott_app_event` A
    LEFT JOIN `cto-wln-sa-data-pr-bb5283.app_cat_map.bq_app_cat_mapping_latest_view` B 
    ON A.app_nm = B.app_nm
    WHERE
      B.tier_1 IN ('Smart Home', 'Shopping', 'News', 'Travel', 'Communication')
      --AND imsi_num = '302220013013525'
      --AND event_dt > '2022-07-31'
      
    GROUP BY A.imsi_num, A.event_dt, CONCAT(REGEXP_REPLACE(TRIM(LOWER(B.tier_1)),r'[^a-zA-Z]',''), '_', REGEXP_REPLACE(TRIM(LOWER(B.tier_2)),r'[^a-zA-Z]',''))
    
  )

  SELECT 
    A.bap_bus_bacct_num AS BAN,
    A.subscriber_num AS MSISDN,
    A.imsi_id,
    A.labels,
    B.event_dt, 
    B.category,
    B.usage_volume_MB
  FROM `roaming-pr-66a1b0.pre_trip_modeling_v2.pre_trip_labels_final` A
  INNER JOIN app_usage_data B 
  ON 
    A.imsi_id = B.imsi_num
    AND B.event_dt BETWEEN DATE_SUB(A.trip_first_date, INTERVAL 7 DAY) AND A.trip_first_date

  -- WHERE 
  --   A.imsi_id IN ('302220013013525', '302220318213893')
  ORDER BY A.imsi_id, B.event_dt, B.category
""";

SET app_usage_categories_pivot_str = (SELECT CONCAT('("', STRING_AGG(DISTINCT category, '", "'), '")') FROM app_usage);

EXECUTE IMMEDIATE format("""
  SELECT
    *
  FROM app_usage
  PIVOT(
      MAX(usage_volume_MB) AS usage_volume_MB
      FOR category in %s
  )
""", app_usage_categories_pivot_str)

