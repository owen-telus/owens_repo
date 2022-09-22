WITH data_allowance AS (
  SELECT
    BILLING_ACCOUNT_NUM AS BAN,
    SUBSCRIBER_NUM AS MSISDN,
    CASE
        WHEN UNIT_OF_MEASURE_CD='GB' THEN ALLOWANCE_QTY
        WHEN UNIT_OF_MEASURE_CD='MB' THEN ALLOWANCE_QTY / 1000.0
        WHEN UNIT_OF_MEASURE_CD='KB' THEN ALLOWANCE_QTY / 1000000.0
        ELSE 0 -- Assume UNIT is not data allowance
    END AS data_allowance_gb,
  FROM `cio-datahub-enterprise-qa-ecf3.ent_cust_cust.bq_subscriber_wls_data_allowance` 
)

-- Other method is to utilize the bq_subscriber_wls_data_allowance table and sum up all the data allowance values for each subscriber

SELECT
  BAN,
  MSISDN,
  SUM(data_allowance_gb) AS data_allowance_gb
FROM data_allowance
GROUP BY BAN, MSISDN 