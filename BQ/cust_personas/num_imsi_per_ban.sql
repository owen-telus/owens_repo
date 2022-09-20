WITH stats_can_data AS ( SELECT * FROM
(
  -- #1 from_item
SELECT 
    GEO_NAME,
    CASE
        WHEN DIM__Profile_of_Forward_Sortation_Areas__2247_ = 'Average household size' THEN 'avg_household_size'
    END AS  metric, 
    Dim__Sex__3___Member_ID___1___Total___Sex AS value
FROM `cto-wln-sa-data-pr-bb5283.ref_table.bq_exttable_statscan_census_2016`
WHERE 
    DIM__Profile_of_Forward_Sortation_Areas__2247_ = 'Average household size'
)
PIVOT
(
  -- #2 aggregate
  AVG(safe_cast(value AS FLOAT64)) AS value
  -- #3 pivot_column
  FOR metric in ('avg_household_size')
)),

ban_fsa AS 
(SELECT distinct BUS_BILLING_ACCOUNT_NUM, SUBSTR(POSTAL_CD, 1,3) AS FSA_CODE from ref_table.bq_hpbi_billing_account_dim)
,

consumer_ban AS (
    SELECT BAN, IMSI, value_avg_household_size
    FROM `cto-wln-sa-data-pr-bb5283.customer_personas_features.cust_mapping` a
    LEFT JOIN ban_fsa b 
    ON 
    a.BAN = b.BUS_BILLING_ACCOUNT_NUM
    LEFT JOIN stats_can_data c 
    ON b.FSA_CODE = c.GEO_NAME
    WHERE customer_type='Consumer'

)

SELECT 
    BAN, 
    COUNT(DISTINCT IMSI) as imsi_count_per_ban, 
    AVG(value_avg_household_size) as avg_household_size
FROM consumer_ban
GROUP BY BAN 
HAVING COUNT(DISTINCT IMSI) > 1 --AND COUNT(DISTINCT  IMSI) < 10  
ORDER BY avg_household_size, imsi_count_per_ban DESC
LIMIT 10


-- SELECT * 
-- FROM stats_can_data
-- ORDER BY value_avg_household_size desc 