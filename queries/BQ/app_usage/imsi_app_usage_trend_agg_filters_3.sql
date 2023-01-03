-- Get Brand info
WITH brand_table AS (
  SELECT 
    A.*,  
    IFNULL(B.BRAND_TXT, 'Other') AS BRAND_TXT
  FROM `cto-wln-sa-data-pr-bb5283.customer_personas_features.imsi_app_usage_trends_weekly_ts_2` A
  LEFT JOIN `cto-wln-sa-data-pr-bb5283.ref_table.bq_hpbi_product_instance_profl` B 
  ON A.imsi_num= B.PROD_INSTNC_ALIAS_STR

),

avg_usg AS (
  SELECT
    ts_index,
    tier_1,
    tier_2,
    BRAND_TXT,
    AVG(CASE WHEN usage_volume_MB <> 0 THEN usage_volume_MB ELSE NULL END) AS usage_avg_MB
  FROM brand_table
  GROUP BY ts_index, tier_1, tier_2, BRAND_TXT

)



SELECT
  A.ts_index,
  A.start_dt,
  A.end_dt,
  A.BRAND_TXT,
  A.tier_1, 
  A.tier_2,
  SUM(A.usage_volume_MB/1000000.0) AS usage_volume_TB,
  COUNTIF(A.usage_volume_MB > 0) AS num_users,
  COUNTIF(A.usage_volume_MB > B.usage_avg_MB) AS num_users_above_avg_usage
FROM brand_table A 
LEFT JOIN avg_usg B
ON A.ts_index = B.ts_index AND A.tier_1 = B.tier_1 AND A.tier_2 = B.tier_2 AND A.BRAND_TXT = B.BRAND_TXT
GROUP BY ts_index, start_dt, end_dt, tier_1, tier_2, BRAND_TXT
ORDER BY ts_index, tier_1