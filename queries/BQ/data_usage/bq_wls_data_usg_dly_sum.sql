-- only has about 2 months worth of data
SELECT MIN(data_usg_dly_sum_dt) FROM `cio-datahub-enterprise-pr-183a.ent_usage_rated.bq_wls_data_usg_dly_sum` 
--WHERE data_usg_dly_sum_dt = "2022-06-01" AND throttle_ind = 'Y'

SELECT 
  subscr_ph_num,
  COUNT(*) as num_throttle_months
FROM (
  SELECT
    COUNT(*),
    subscr_ph_num,
    billg_cycl_yr_num,
    billg_cycl_mth_num,
    throttle_ind
  FROM 
    `cio-datahub-enterprise-pr-183a.ent_usage_rated.bq_wls_data_usg_dly_sum` 
  WHERE throttle_ind = 'Y' AND data_usg_dly_sum_dt > "2021-01-01" AND subscr_ph_num='2042021797'
  GROUP BY subscr_ph_num, billg_cycl_yr_num, billg_cycl_mth_num, throttle_ind
  ORDER BY subscr_ph_num
)
GROUP BY subscr_ph_num,billg_cycl_yr_num

SELECT COUNT(DISTINCT subscr_ph_num)
FROM `cio-datahub-enterprise-pr-183a.ent_usage_rated.bq_wls_data_usg_dly_sum` 
WHERE data_usg_dly_sum_dt > "2022-05-01" AND throttle_ind = 'Y'


SELECT * FROM `cio-datahub-enterprise-pr-183a.ent_usage_rated.bq_wls_data_usg_dly_sum` 
WHERE data_usg_dly_sum_dt > "2022-04-01" AND subscr_ph_num='6047989340'
ORDER BY data_usg_dly_sum_dt


SELECT *  FROM `cto-wln-sa-data-pr-bb5283.ref_table.bq_exttable_hpbi_product_instance_profl` 
WHERE PROD_INSTNC_RESRC_STR='6047989340'

SELECT * FROM `cio-datahub-enterprise-pr-183a.ent_usage_rated.bq_wls_data_usg_dly_sum` 
WHERE data_usg_dly_sum_dt > "2022-05-01" AND og_sms_ca_na_actl_unit_qty > 1
ORDER BY data_usg_dly_sum_dt