WITH MOVIE_WATCH_STAGE AS (
    SELECT
        A.tv_pgm_nm AS MOVIE,
        B.tv_pgm_nm AS MOST_WATCHED,
        A.tv_acct_extrnl_id AS ID
                
    FROM `cio-datahub-enterprise-pr-183a.ent_usage_rated_tv.bq_tv_program_watch_dly_sum` A
    INNER JOIN `cio-datahub-enterprise-pr-183a.ent_usage_rated_tv.bq_tv_program_watch_dly_sum` B
    ON A.tv_acct_extrnl_id = B.tv_acct_extrnl_id AND A.tv_pgm_nm <> B.tv_pgm_nm
    WHERE 
        A.tv_pgm_watch_dly_sum_dt > '2022-01-01' -- Need to have a date filter otherwise query wont run
        AND A.tv_media_catgy_txt='Movies'
        AND A.tv_media_subcatgy_txt <> 'Adults Only' -- No Adult Content
        AND A.tv_watch_max_pct > 50 -- Watched greater than 50% of the movie
        AND A.tv_pgm_lang_cd='en' -- ENGLISH Movies Only

        AND B.tv_pgm_watch_dly_sum_dt > '2022-01-01' -- Need to have a date filter otherwise query wont run
        AND B.tv_media_catgy_txt='Movies'
        AND B.tv_media_subcatgy_txt <> 'Adults Only' -- No Adult Content
        AND B.tv_watch_max_pct > 50 -- Watched greater than 50% of the movie
        AND B.tv_pgm_lang_cd='en' -- ENGLISH Movies Only

),

MOVIE_WATCH AS (
    SELECT 
        MOVIE,
        MOST_WATCHED,
        COUNT(DISTINCT ID) AS num_views,
        DENSE_RANK() OVER (PARTITION BY MOVIE ORDER BY COUNT(DISTINCT ID) DESC) AS rank
    FROM MOVIE_WATCH_STAGE
    GROUP BY MOVIE, MOST_WATCHED
    
)

SELECT *
FROM MOVIE_WATCH
WHERE 
    rank <= 5 
    AND num_views > 100
ORDER BY MOVIE, rank