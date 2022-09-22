-- Query Returns tv usage in minutes by weekend and weekday, and by tv_media_subcategory for the past month
-- BAN's are all FFH BAN's with src_id = 1001
-- If you have 'data is not located in north america' error, change the query settings (More -> Query Settings -> Data Location -> Select Montreal)
-- Requires access to DAG_TV


DECLARE tv_media_categories_pivot_str STRING;

EXECUTE IMMEDIATE """
  CREATE TEMP TABLE TEMP_TV_USAGE_STAGE AS 
    WITH usage AS (
      SELECT 
        tv_pgm_watch_dly_sum_dt,
        CASE WHEN EXTRACT(DAYOFWEEK FROM tv_pgm_watch_dly_sum_dt) IN (1, 7) THEN "weekend"  ELSE "weekday" END AS day,
        bus_bacct_num,
        bus_bacct_num_src_id,

        REGEXP_REPLACE(TRIM(LOWER(tv_media_subcatgy_txt)),r'[^a-zA-Z]','') AS tv_media_subcatgy_txt,
        tv_watch_tot_sec_qty/ 60.0 AS tv_watch_tot_mins,

      FROM `cio-datahub-enterprise-pr-183a.ent_usage_rated_tv.bq_tv_program_watch_dly_view`
      WHERE 
        tv_pgm_watch_dly_sum_dt >  DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH) -- Change filter for number of days 
        AND tv_media_subcatgy_txt is NOT NULL 
    )
    SELECT 
      bus_bacct_num,
      bus_bacct_num_src_id,
      CONCAT(tv_media_subcatgy_txt, '_', day) AS category_col,
      SUM(tv_watch_tot_mins) AS total_tv_mins
    FROM usage
    GROUP BY 
      CONCAT(tv_media_subcatgy_txt, '_', day), bus_bacct_num, bus_bacct_num_src_id
  """ ;
SET tv_media_categories_pivot_str = (SELECT CONCAT('("', STRING_AGG(DISTINCT category_col, '", "'), '")') FROM TEMP_TV_USAGE_STAGE);

EXECUTE IMMEDIATE format("""
    CREATE TEMP TABLE TEMP_TV_USAGE AS(
    SELECT 
      * 
    FROM 
      TEMP_TV_USAGE_STAGE
    PIVOT(
      MAX(total_tv_mins) AS tv_watch_tot_mins
      FOR category_col IN %s)
    )
  """, tv_media_categories_pivot_str );


SELECT *
FROM TEMP_TV_USAGE