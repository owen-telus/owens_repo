WITH soc_rates_table AS (
SELECT 
    soc,
    rate,
    
    'additional_charge' AS charge_type
FROM `cio-datahub-enterprise-pr-183a.src_kb.bq_pp_ac_rate`
WHERE snapshot_load_ts = (SELECT MAX(snapshot_load_ts) FROM `cio-datahub-enterprise-pr-183a.src_kb.bq_pp_ac_rate` )

UNION ALL

SELECT 
    soc,
    rate,

    'one_time_charge' AS charge_type
FROM `cio-datahub-enterprise-pr-183a.src_kb.bq_pp_oc_rate`
WHERE snapshot_load_ts = (SELECT MAX(snapshot_load_ts) FROM `cio-datahub-enterprise-pr-183a.src_kb.bq_pp_oc_rate` )

UNION ALL

SELECT 
    soc,
    rate,
    'usage_charge' AS charge_type
FROM `cio-datahub-enterprise-pr-183a.src_kb.bq_pp_uc_rate`
WHERE snapshot_load_ts = (SELECT MAX(snapshot_load_ts) FROM `cio-datahub-enterprise-pr-183a.src_kb.bq_pp_uc_rate` )

UNION ALL


SELECT 
    soc,
    CAST(rate AS NUMERIC) AS rate,
    'recurring_charge' AS charge_type
FROM `cio-datahub-enterprise-pr-183a.src_kb.bq_pp_rc_rate`
WHERE snapshot_load_ts = (SELECT MAX(snapshot_load_ts) FROM `cio-datahub-enterprise-pr-183a.src_kb.bq_pp_rc_rate` )

),

soc_table AS (
    SELECT 
        A.* ,
        B.effective_date,
        B.soc_description,
        B.service_type,
        B.soc_level_code
    FROM soc_rates_table A 
    FULL OUTER JOIN `cio-datahub-enterprise-pr-183a.src_kb.bq_soc` B
    ON A.soc = B.soc 
    WHERE B.snapshot_load_ts = (SELECT MAX(snapshot_load_ts) FROM `cio-datahub-enterprise-pr-183a.src_kb.bq_soc`)

)

SELECT * FROM soc_table
WHERE soc in  ('3SOTTSTPL','SOTTSTPL','XSEOTTPL','XSOTTBPL', 'SOTTSTPL','SMHOTTPLR','SMHOTTPLP','SOTTSTR ','SOTTSTPP','SOTTSTPLR','SOTTSTPLP','SWBOTTPLR','SWBOTTPLP') -- Stream+