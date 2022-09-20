SELECT PRIM_PROD_INSTNC_RESRC_STR, COUNT(*) 
FROM `cto-wln-sa-data-pr-bb5283.ref_table.bq_exttable_hpbi_mthly_prod_instanc_device` 
GROUP BY PRIM_PROD_INSTNC_RESRC_STR
ORDER BY COUNT(*) DESC


SELECT A.*, B.days_since_launch
FROM `cto-wln-sa-data-pr-bb5283.ref_table.bq_exttable_hpbi_mthly_prod_instanc_device` A
LEFT JOIN `cto-wln-sa-data-pr-bb5283.customer_personas_features.tac_info` B 
ON SUBSTR(CAST(A.DEVICE_EQUIP_SERIAL_NUM AS STRING), 0, 8) = B.tac_id
WHERE B.days_since_launch IS NOT NULL

WITH device_changes_stage AS (
  SELECT 
    A.*, 
    B.days_since_launch,
    LAG(days_since_launch) OVER(PARTITION BY BUS_BILLING_ACCOUNT_NUM, PRIM_PROD_INSTNC_RESRC_STR ORDER BY PROD_INSTNC_MTHLY_PROFL_TS ASC) AS prev_device_days_since_launch
  FROM `cto-wln-sa-data-pr-bb5283.ref_table.bq_exttable_hpbi_mthly_prod_instanc_device` A
  LEFT JOIN `cto-wln-sa-data-pr-bb5283.customer_personas_features.tac_info` B 
  ON SUBSTR(CAST(A.DEVICE_EQUIP_SERIAL_NUM AS STRING), 0, 8) = B.tac_id
  --WHERE A.PRIM_PROD_INSTNC_RESRC_STR IN (4384974686, 2508572158, 7805029236, 4182919917,2506180026 , 9022936533, 7807185536)
  WHERE DEVICE_EQUIP_SERIAL_NUM IS NOT NULL
),

device_changes AS (
  SELECT 
      *,
      CASE 
        WHEN prev_device_days_since_launch > days_since_launch THEN 'new_device' -- Customer switched to new device
        WHEN prev_device_days_since_launch < days_since_launch THEN 'old_device' -- Customer switched to older device
        ELSE 'n/a'
      END AS switched_new_device
  FROM device_changes_stage
),

device_change_pivoted AS (
  SELECT *
  FROM (
    SELECT
      BUS_BILLING_ACCOUNT_NUM,
      PRIM_PROD_INSTNC_RESRC_STR,
      switched_new_device,
      COUNT(*) as num_changes
    FROM device_changes
    GROUP BY BUS_BILLING_ACCOUNT_NUM, PRIM_PROD_INSTNC_RESRC_STR, switched_new_device
  )
  PIVOT(
    AVG(num_changes) for switched_new_device in ('new_device', 'old_device')
  )

)


SELECT *
FROM device_change_pivoted 
WHERE 
  new_device IS NOT NULL OR old_device IS NOT NULL


--WHERE switched_new_device ='old_device'



SELECT * 
FROM `cto-wln-sa-data-pr-bb5283.customer_personas_features.tac_info` B 
 WHERE days_since_launch IS NOT NULL