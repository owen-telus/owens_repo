DECLARE categories STRING ; 
EXECUTE IMMEDIATE """
  CREATE TEMP TABLE agg_data AS 
    WITH brand_renamed AS (
      SELECT 
        * EXCEPT(BRAND_TXT),
        CASE WHEN BRAND_TXT = 'Telus regular brand' THEN 'TELUS'
            WHEN BRAND_TXT = 'Koodo Mobile' THEN 'KOODO'
            ELSE BRAND_TXT
        END AS BRAND_TXT 

      FROM `cto-wln-sa-data-pr-bb5283.customer_personas_features.imsi_app_usage_trends_agg_ts_2`
    )

    SELECT 
      ts_index,
      start_dt,
      end_dt, 
      CONCAT(REGEXP_REPLACE(TRIM(LOWER(tier_1)),r'[^a-zA-Z]',''), '_', REGEXP_REPLACE(TRIM(LOWER(tier_2)),r'[^a-zA-Z]',''), '_', TRIM(BRAND_TXT) ) AS category,

      usage_volume_TB,
      num_users,
      num_users_above_avg_usage
    FROM brand_renamed

""";

SET categories = (SELECT CONCAT('("', STRING_AGG(DISTINCT category, '", "'), '")'), FROM agg_data);

EXECUTE IMMEDIATE format("""  
  SELECT *
  FROM agg_data
  PIVOT(
    MAX(usage_volume_TB) AS usage_volume_TB,
    MAX(num_users) AS num_users,
    MAX(num_users_above_avg_usage) AS num_users_above_avg_usage
    FOR category in %s
  )

""", categories);