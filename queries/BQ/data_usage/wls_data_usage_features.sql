WITH wls_data_usage_stage AS (
  SELECT
    ban as mob_ban,
    subscr_ph_num as msisdn,
    data_usg_dly_sum_dt,
    bill_cycle_clos_dt,
    --wisp_chg_amt,
    wisp_actl_kb_qty, -- total data usage
    hm_wisp_actl_kb_qty, -- domestic data usage
    --hm_wisp_chg_amt,
    rm_wisp_actl_kb_qty, -- roaming data usage
    --rm_wisp_chg_amt,
    CASE WHEN throttle_ind = 'Y'
    THEN 1 
    ELSE 0 END AS throttled_bool,
    
    -- 1 will be most recent month
    DENSE_RANK() OVER(PARTITION BY ban, subscr_ph_num ORDER BY billg_cycl_yr_num DESC, billg_cycl_mth_num DESC) AS row_num,
    -- Find date of max data usage by billing cycle 
    DENSE_RANK() OVER(PARTITION BY ban, subscr_ph_num, billg_cycl_yr_num , billg_cycl_mth_num  ORDER BY wisp_actl_kb_qty  DESC) AS max_data_used_day_row_num 
  FROM `cio-datahub-enterprise-pr-183a.ent_usage_rated.bq_wls_data_usg_dly_sum` 
  -- Replace CURRENT_DATE() with part_dt (partition date)
  WHERE data_usg_dly_sum_dt <= CURRENT_DATE() AND data_usg_dly_sum_dt >= DATE_SUB(CURRENT_DATE(), INTERVAL 4 MONTH)
  AND subscr_ph_num = '7785545398'
),

wls_data_usage_stage_agg_1 AS (
  SELECT
    mob_ban,
    msisdn,
    row_num,
    MAX(bill_cycle_clos_dt) AS bill_cycle_clos_dt,
    SUM(throttled_bool) AS num_days_data_throttled,
    
    SUM(wisp_actl_kb_qty /1000000.0) AS total_data_used_gb,
    SUM(hm_wisp_actl_kb_qty /1000000.0) AS total_hm_data_used_gb,
    SUM(rm_wisp_actl_kb_qty /1000000.0) AS total_rm_data_used_gb,
    MAX(wisp_actl_kb_qty /1000000.0) AS max_daily_data_used_gb
  FROM wls_data_usage_stage
  WHERE row_num <= 4 
  GROUP BY mob_ban,msisdn, row_num
),

-- Add Date of Max Data Usage
wls_data_usage_stage_agg_2 AS (
  SELECT
    A.*, 
    B.data_usg_dly_sum_dt AS max_daily_data_used_date
  FROM wls_data_usage_stage_agg_1 A 
  LEFT JOIN wls_data_usage_stage B 
  ON A.mob_ban = B.mob_ban AND A.msisdn = B.msisdn AND A.bill_cycle_clos_dt = B.bill_cycle_clos_dt
  WHERE B.max_data_used_day_row_num = 1
),

wls_data_usage_pivoted AS (
  SELECT * 
  FROM wls_data_usage_stage_agg_2 
  PIVOT(
    MAX(bill_cycle_clos_dt) AS bill_cycle_clos_dt,
    MAX(num_days_data_throttled) AS num_days_data_throttled,
    MAX(max_daily_data_used_gb) AS max_daily_data_used_gb,
    MAX(total_data_used_gb) AS total_data_used_gb,
    MAX(total_hm_data_used_gb) AS total_hm_data_used_gb,
    MAX(total_rm_data_used_gb) AS total_rm_data_used_gb,
    MAX(max_daily_data_used_date) AS max_daily_data_used_date
    FOR row_num IN (1,2,3,4)
  )

), 

wls_data_usage_features AS (
  SELECT 
    mob_ban,
    CAST(msisdn AS INT64) AS msisdn,
    bill_cycle_clos_dt_1,
    bill_cycle_clos_dt_2,
    bill_cycle_clos_dt_3,
    num_days_data_throttled_1, 
    num_days_data_throttled_2,
    num_days_data_throttled_3,
    max_daily_data_used_gb_1,
    max_daily_data_used_gb_2,
    max_daily_data_used_gb_3,
    max_daily_data_used_date_1,
    max_daily_data_used_date_2,
    max_daily_data_used_date_3,
    total_data_used_gb_1,
    total_data_used_gb_2,
    total_data_used_gb_3,
    total_hm_data_used_gb_1,
    total_hm_data_used_gb_2,
    total_hm_data_used_gb_3,
    total_rm_data_used_gb_1,
    total_rm_data_used_gb_2,
    total_rm_data_used_gb_3
  FROM wls_data_usage_pivoted
)


SELECT * FROM wls_data_usage_features
