-- SELECT 
--   throttle_ind,
--   count(*)
-- FROM `cio-datahub-enterprise-pr-183a.ent_usage_rated.bq_wls_data_usg_dly_sum` 
-- WHERE data_usg_dly_sum_dt = "2022-11-27" 
-- group by 1 


-- SELECT 
--   *
-- FROM `cio-datahub-enterprise-pr-183a.ent_usage_rated.bq_wls_data_usg_dly_sum` 
-- WHERE data_usg_dly_sum_dt = "2022-11-27" 
-- AND throttle_ind='Y'


-- SELECT *
-- FROM `cio-datahub-enterprise-pr-183a.ent_usage_rated.bq_wls_data_usg_dly_sum` 
-- WHERE subscr_ph_num='5878883006'
-- ORDER BY data_usg_dly_sum_dt DESC 

-- Create feature that shows number of days a customers usage was throttled by billing cycle, take past 3 billing cycles


WITH num_throttle_days_stage AS (

  SELECT
    ban AS BAN,
    subscr_ph_num AS MSISDN,
    data_usg_dly_sum_dt,
    bill_cycle_clos_dt,
    wisp_chg_amt,
    wisp_actl_kb_qty,
    CASE WHEN throttle_ind = 'Y' 
    THEN 1 
    ELSE 0 END AS throttle_bool,
    DENSE_RANK() OVER(PARTITION BY ban, subscr_ph_num ORDER BY billg_cycl_yr_num DESC, billg_cycl_mth_num DESC) as row_num -- 1 will be most recent cycle
    
  FROM `cio-datahub-enterprise-pr-183a.ent_usage_rated.bq_wls_data_usg_dly_sum` 
  WHERE subscr_ph_num=  '6479997480' --   '2043339222' -- something wrong with this table, there is a delay
  ORDER BY data_usg_dly_sum_dt DESC
         

),

num_throttle_days AS (

  SELECT 
    BAN,
    MSISDN,
    row_num,
    MAX(bill_cycle_clos_dt) AS bill_cycle_clos_dt,
    SUM(throttle_bool) AS num_days_throttled,
    SUM(wisp_chg_amt) AS data_overcharge_amt,
    MAX(wisp_actl_kb_qty /1000000.0) AS max_daily_data_used_gb,
    SUM(wisp_actl_kb_qty /1000000.0) AS data_used_gb
  FROM num_throttle_days_stage
  WHERE row_num <= 4
  GROUP BY BAN, MSISDN, row_num
),

num_throttle_days_pivoted AS (

  SELECT *
  FROM num_throttle_days
  PIVOT (
    MAX(bill_cycle_clos_dt) AS bill_cycle_clos_dt,
    MAX(num_days_throttled) AS num_days_throttled,
    MAX(data_overcharge_amt) AS data_overcharge_amt,
    MAX(max_daily_data_used_gb) AS max_daily_data_used_gb,
    MAX(data_used_gb) AS data_used_gb
    FOR row_num IN (1,2,3, 4) 
  )
),

num_throttle_days_pivoted_days AS (

  SELECT 
    *,
    CASE WHEN CURRENT_DATE() >= bill_cycle_clos_dt_2 
    THEN DATE_DIFF(bill_cycle_clos_dt_1 , bill_cycle_clos_dt_2, DAY) 
    ELSE DATE_DIFF(CURRENT_DATE() , bill_cycle_clos_dt_2, DAY) -- Else in current cycle and use current_date to get days used in this cycle
    END AS current_period_days,

    DATE_DIFF(bill_cycle_clos_dt_2 , bill_cycle_clos_dt_3, DAY) AS second_period_days,
    DATE_DIFF(bill_cycle_clos_dt_3 , bill_cycle_clos_dt_4, DAY) AS third_period_days

  FROM num_throttle_days_pivoted
)


SELECT 

  BAN,
  MSISDN,
  data_overcharge_amt_1,
  data_overcharge_amt_2,
  data_overcharge_amt_3,
  max_daily_data_used_gb_1,
  max_daily_data_used_gb_2,
  max_daily_data_used_gb_3,
  data_used_gb_1, -- Current t Month
  data_used_gb_2, -- t-1 Month
  data_used_gb_3, -- t-2 Month

  -- bill_cycle_clos_dt_1, 
  -- bill_cycle_clos_dt_2,
  -- num_days_throttled_1 / current_period_days AS perc_days_throttled_1,
  -- num_days_throttled_2 / second_period_days AS perc_days_throttled_2,
  -- num_days_throttled_3 / third_period_days AS perc_days_throttled_3

  -- Due to billing changes / edge cases

  CASE WHEN num_days_throttled_1 > current_period_days
  THEN 1 
  ELSE num_days_throttled_1 / current_period_days 
  END AS perc_days_throttled_1, 

  CASE WHEN num_days_throttled_2 > second_period_days
  THEN 1 
  ELSE num_days_throttled_2 / second_period_days 
  END AS perc_days_throttled_2, 

  CASE WHEN num_days_throttled_3 > third_period_days
  THEN 1 
  ELSE num_days_throttled_3 / third_period_days 
  END AS perc_days_throttled_3, 

FROM num_throttle_days_pivoted_days
ORDER BY perc_days_throttled_1 DESC 

-- No data to be displayed from below queries meaning there is no division by 0 
-- SELECT * 
-- FROM num_throttle_days_pivoted_days
-- WHERE current_period_days = 0 OR second_period_days = 0 OR third_period_days = 0