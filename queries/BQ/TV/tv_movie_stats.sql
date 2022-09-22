-- Query to get the number of views, average watch percentage, median watch percentage for Movies by Quarter 

DECLARE time_period_pivot_str STRING;

EXECUTE IMMEDIATE """
  CREATE TEMP TABLE MOVIE_STATS AS 

    WITH MOVIE_TABLE AS (
      SELECT
        tv_pgm_nm,
        tv_watch_max_pct,
        CONCAT(EXTRACT(YEAR FROM tv_pgm_watch_dly_sum_dt), '_Q', EXTRACT(QUARTER FROM tv_pgm_watch_dly_sum_dt)) AS time_period        
      FROM `cio-datahub-enterprise-pr-183a.ent_usage_rated_tv.bq_tv_program_watch_dly_sum` 
      WHERE 
        tv_pgm_watch_dly_sum_dt <> CURRENT_DATE() -- Need to have a date filter otherwise query wont run
        AND tv_media_catgy_txt='Movies'
        AND tv_media_subcatgy_txt <> 'Adults Only' -- No Adult Content
        AND tv_watch_max_pct > 50 -- Watched greater than 50% of the movie
        AND tv_pgm_lang_cd='en' -- ENGLISH Movies Only
    )

    SELECT 
      tv_pgm_nm,
      time_period,
      COUNT(*) as num_views,
      ROUND(MAX(median_watch_pct), 2) AS median_watch_pct,
      ROUND(AVG(tv_watch_max_pct), 2) AS average_watch_pct
      
    FROM (
      -- This sub query calculates the median using PERCENTILE_DISC() Function and cannot be GROUP BY in the same subquery
      SELECT
        *,
        PERCENTILE_DISC(tv_watch_max_pct, 0.5) OVER (PARTITION BY tv_pgm_nm, time_period) AS median_watch_pct
      FROM MOVIE_TABLE

    )
    GROUP BY tv_pgm_nm, time_period
    ORDER BY COUNT(*) DESC
  """;

  SET time_period_pivot_str = (SELECT CONCAT('("', STRING_AGG(DISTINCT time_period, '", "'), '")') FROM MOVIE_STATS);

  EXECUTE IMMEDIATE format("""
  
    SELECT 
      *
    FROM MOVIE_STATS
    PIVOT(
        MAX(num_views) AS num_views,
        MAX(average_watch_pct) AS average_watch_pct,
        MAX(median_watch_pct) AS median_watch_pct
        FOR time_period in %s
    )
    """, time_period_pivot_str
  );







