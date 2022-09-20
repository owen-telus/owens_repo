WITH stats_can_income AS ( SELECT * FROM
(
  -- #1 from_item
    SELECT 
        GEO_NAME,
    
        CASE
            WHEN DIM__Profile_of_Forward_Sortation_Areas__2247_ = 'Population, 2016)' THEN 'population'
            WHEN DIM__Profile_of_Forward_Sortation_Areas__2247_ = 'Population density per square kilometre' THEN 'population_density_per_km'
        END AS  metric, 
        Dim__Sex__3___Member_ID___1___Total___Sex AS value 
    FROM `cto-wln-sa-data-pr-bb5283.ref_table.bq_exttable_statscan_census_2016`
    WHERE 
        DIM__Profile_of_Forward_Sortation_Areas__2247_ IN ('Population, 2016)','Population density per square kilometre') 
        -- AND 
       -- GEO_NAME = 'A1S'
)
PIVOT
(
  -- #2 aggregate
  SUM(safe_cast(value AS INT64)) AS value
  -- #3 pivot_column
  FOR metric in ('population','population_density_per_km')
)),

ban_fsa AS 
(SELECT distinct BUS_BILLING_ACCOUNT_NUM, SUBSTR(POSTAL_CD, 1,3) AS FSA_CODE 
FROM ref_table.bq_hpbi_billing_account_dim
WHERE 
    SUBSTR(POSTAL_CD, 1,3) = 'V6X'
)


select * except (GEO_NAME) 
FROM stats_can_income
--from ban_fsa a 
-- inner join stats_can_income  b 
-- on 
-- a.FSA_CODE = b.GEO_NAME
