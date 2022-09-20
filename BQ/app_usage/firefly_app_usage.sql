WITH 

user_data as (
    SELECT 
        imsi_num,
        dl_volume_qty / 1000000.0 as dl_volume_mb,
        app_nm,

        
    FROM 
        `cio-datahub-enterprise-pr-183a.ent_usage_unrated_ott.bq_ott_app_event`

    WHERE 
        imsi_num LIKE '302%' AND 
        whsia_flag_txt = 'N'AND 
        DATE(event_dt) >= '2022-02-17' AND DATE(event_dt) < '2022-03-17' 
        AND app_nm in ('apple adaptive http video', 'amazon video', 'netflix video', 'disney plus')
)

SELECT * FROM user_data;