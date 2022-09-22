-- Query to determine a customer's data usage based on the current cycle.
-- Data allowance table: bq_subscriber_wls_data_allowance may be incomplete as it is currently in qa
-- Data usage is aggregated on a daily level from bq_wls_data_usg_dly_sum. This table may also contain some errors, incorrect values for cycle_end_date

WITH data_allowance_table AS (
  SELECT 
    BILLING_ACCOUNT_NUM AS BAN,
    SUBSCRIBER_NUM AS MSISDN,
    -- Change allowance to gigabyte
    CASE 
      WHEN UNIT_OF_MEASURE_CD='GB' THEN ALLOWANCE_QTY
      WHEN UNIT_OF_MEASURE_CD='MB' THEN ALLOWANCE_QTY / 1000.0
      WHEN UNIT_OF_MEASURE_CD='KB' THEN ALLOWANCE_QTY / 1000000.0
      ELSE 0
    END AS data_allowance_gb,
    UNIT_OF_MEASURE_CD
     
  FROM `cio-datahub-enterprise-qa-ecf3.ent_cust_cust.bq_subscriber_wls_data_allowance` 
  WHERE 
    PRICE_TYPE_CD='CONTRIBUTION'
    AND RECURRING_TYPE_CD = 'MONTHLY'

),

current_cycle_data_used_stage AS (

  SELECT
    ban AS BAN,
    subscr_ph_num as MSISDN,
    bill_cycle_clos_dt,
    wisp_actl_kb_qty,
    throttle_ind,
    DENSE_RANK() OVER(PARTITION BY ban, subscr_ph_num ORDER BY billg_cycl_yr_num DESC, billg_cycl_mth_num DESC) as row_num -- 1 will be most recent cycle
    
  FROM `cio-datahub-enterprise-pr-183a.ent_usage_rated.bq_wls_data_usg_dly_sum` 

),

current_cycle_data_used AS (
  SELECT
    BAN,
    MSISDN,
    MAX(bill_cycle_clos_dt) AS cycle_end_date,
    SUM(wisp_actl_kb_qty /1000000.0) AS gb_data_used,
    MAX(throttle_ind) AS throttle_ind 

  FROM current_cycle_data_used_stage
  WHERE
    row_num = 1
  GROUP BY BAN, MSISDN, row_num
)

SELECT
  A.BAN,
  A.MSISDN,
  A.cycle_end_date,
  DATE_DIFF(DATE(A.cycle_end_date) , CURRENT_DATE() , DAY) AS days_until_cycle_end,
  ROUND(A.gb_data_used, 2) AS gb_data_used,
  CASE 
    WHEN B.data_allowance_gb > 0 THEN ROUND(A.gb_data_used / B.data_allowance_gb*100, 2) 
    ELSE NULL
  END AS percent_data_used,
  B.data_allowance_gb,
  throttle_ind
FROM current_cycle_data_used A
LEFT JOIN data_allowance_table B -- data allowance table may be incomplete
ON A.BAN = B.BAN AND A.MSISDN = B.MSISDN
ORDER BY percent_data_used DESC 
