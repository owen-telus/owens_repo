DECLARE num_categories int64; 
DECLARE app_usage_categories ARRAY<STRUCT<categories STRING>>;
DECLARE i INT64 DEFAULT 0;
DECLARE sum_app_category_string STRING;

EXECUTE IMMEDIATE """

  CREATE TEMP TABLE app_usage_stage AS
    SELECT
      DISTINCT tier_1 AS category

    FROM `cio-datahub-enterprise-pr-183a.ent_usage_unrated_ott.bq_ott_app_event` A
    LEFT JOIN `cto-wln-sa-data-pr-bb5283.app_cat_map.bq_app_cat_mapping_latest_view` B 
    ON A.app_nm = B.app_nm
    WHERE
      B.tier_1 IN ('Smart Home', 'Shopping', 'News', 'Travel', 'Communication') -- Relevant App Categories
      AND A.imsi_num = '302220317604594'
      AND A.event_dt > '2022-08-01'


""";

SET num_categories = (SELECT COUNT(DISTINCT category) FROM app_usage_stage);
SET app_usage_categories = (
  WITH distinct_categories AS(
    SELECT DISTINCT category as unique_categories FROM app_usage_stage
  )
  SELECT ARRAY_AGG(STRUCT(unique_categories)) FROM distinct_categories
);

WHILE i < num_categories DO
  SET sum_app_category_string = STRING_AGG('SUM(', app_usage_categories[i], ')');
  SET i = i + 1;

END WHILE;

