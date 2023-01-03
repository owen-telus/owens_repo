  -- Delinquencies over time

WITH change_in_col AS (
  SELECT 
    prod_instnc_ts,
    bacct_bus_bacct_num,
    IFNULL(bacct_delinq_ind, 'N') AS bacct_delinq_ind, 
    ROW_NUMBER() OVER(PARTITION BY bacct_bus_bacct_num, IFNULL(bacct_delinq_ind, 'N') ORDER BY prod_instnc_ts ASC ) AS ROW_NUM
  FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht`
  WHERE DATE(prod_instnc_ts) >= '2022-11-01'
  AND bacct_bus_bacct_num=39877737.0	
)


-- If bacct_delinq_ind = 'Y' then customer went from N to Y
-- Else, customer went from Y to N
-- Need to do a rolling count + Add number of customers that are already delinquent prior to start date
-- If customers first entry is not the start date, then we will get duplicate entries.
-- Basically need a dynamic filter to filter out first date for that customer, maybe there is a better approach
-- will think about it
SELECT 
  *
FROM change_in_col 
WHERE ROW_NUM = 1 
AND DATE(prod_instnc_ts) <> '2022-11-01' -- Removes start date


-- SELECT 
--   prod_instnc_ts,
--   bacct_bus_bacct_num,
--   IFNULL(MAX(bacct_delinq_ind), 'N') AS bacct_delinq_ind

-- FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht`
-- WHERE DATE(prod_instnc_ts) > '2022-11-01'
-- AND bacct_bus_bacct_num=38311116.0	
-- GROUP BY 1, 2
-- ORDER BY prod_instnc_ts ASC 