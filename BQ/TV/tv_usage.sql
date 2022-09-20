DECLARE tv_media_categories_pivot_str STRING;

EXECUTE IMMEDIATE """
  CREATE TEMP TABLE temp_tv_usage AS 
    WITH usage AS (
      SELECT 
        tv_pgm_watch_dly_sum_dt,
        CASE WHEN EXTRACT(DAYOFWEEK FROM tv_pgm_watch_dly_sum_dt) IN (1, 7) THEN "weekend"  ELSE "weekday" END AS day,
        bus_bacct_num,
        REGEXP_REPLACE(TRIM(LOWER(tv_media_subcatgy_txt)),r'[^a-zA-Z]','') AS tv_media_subcatgy_txt,
        tv_watch_tot_sec_qty/ 60.0 AS tv_watch_tot_mins,
        --SUM(tv_watch_tot_cnt) AS tv_watch_tot_cnt,

      FROM `cio-datahub-enterprise-pr-183a.ent_usage_rated_tv.bq_tv_program_watch_dly_view`
      WHERE 
        -- bus_bacct_num=604690167
        tv_pgm_watch_dly_sum_dt > 2022-05-22
        AND tv_media_subcatgy_txt is NOT NULL 
    )
    SELECT 
      bus_bacct_num,
      CONCAT(tv_media_subcatgy_txt, '_', day) AS category_col,
      SUM(tv_watch_tot_mins) AS total_tv_mins
    FROM usage
    GROUP BY 
      CONCAT(tv_media_subcatgy_txt, '_', day), bus_bacct_num
  """ ;
SET tv_media_categories_pivot_str = (SELECT CONCAT('("', STRING_AGG(DISTINCT category_col, '", "'), '")') FROM temp_tv_usage);

EXECUTE IMMEDIATE format("""
    
    SELECT 
      * 
    FROM 
      temp_tv_usage
    PIVOT(
      MAX(total_tv_mins) AS tv_watch_tot_mins
      FOR category_col IN %s)
  """, tv_media_categories_pivot_str );
