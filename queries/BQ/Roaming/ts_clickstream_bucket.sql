
-- FROM EDA, I see a trend where customers who have clickstream events are travelling
-- Create Table with BAN, MSISDN, IMSI, ts_index (Which essentially is the time step and Click Event
-- Time step will be 8
-- I will create buckets where for each time step, it will cover a period of 3 days for clickstream, since these events are more rare
-- Ie. for ts_index = 8, the clickstream_event_ts will be between trip_start_date and trip_start_date - 3 days
-- Ie. for ts_index = 7, the clickstream_event_ts will be between trip_start_date - 3 days and trip_start_date - 6 days

WITH trip_info_date_bucketed AS (
  SELECT
    A.bap_bus_bacct_num AS BAN,
    A.subscriber_num AS MSISDN,
    A.imsi_id,
    A.trip_first_date,
    ROW_NUMBER() OVER(PARTITION BY A.bap_bus_bacct_num, A.subscriber_num, A.imsi_id, A.trip_first_date ORDER BY start_dt ASC) AS ts_index,
    -- Create Date Buckets
    start_dt,
    DATE_ADD(start_dt, INTERVAL 3 DAY) AS end_dt,
  FROM `roaming-pr-66a1b0.pre_trip_modeling_v2.pre_trip_labels_final` A, UNNEST(GENERATE_DATE_ARRAY( DATE_SUB(A.trip_first_date, INTERVAL 8*3  DAY), DATE_SUB(A.trip_first_date, INTERVAL 3 DAY), INTERVAL 3 DAY ) ) AS start_dt

),

clickstream_agg AS (

  SELECT 
    B.bus_bacct_num,
    DATE(C.clckstrm_event_ts) AS clckstrm_event_ts,
    COUNT(DISTINCT C.clckstrm_event_id) AS click_count
  FROM `cio-datahub-enterprise-pr-183a.ent_party_identity.bq_cust_idnty_billg_acct_snpsht` B
  LEFT JOIN `cio-datahub-enterprise-pr-183a.ent_cust_intractn.bq_clckstrm_telus_web_event` C
  ON B.uuid = C.uuid
  WHERE
    (C.pg_hit_url_str LIKE '%/mobility%' AND C.pg_nm LIKE '%travel%' AND C.pg_nm NOT LIKE '%business%')
    AND B.cust_idnty_billg_acct_ts = (SELECT MAX(cust_idnty_billg_acct_ts) FROM `cio-datahub-enterprise-pr-183a.ent_party_identity.bq_cust_idnty_billg_acct_snpsht`)
  GROUP BY B.bus_bacct_num, C.clckstrm_event_ts
),

clickstream_table_final AS (
  SELECT
    A.BAN,
    A.MSISDN,
    A.imsi_id,
    A.trip_first_date,
    A.ts_index,
    A.start_dt,
    A.end_dt,
    IFNULL(SUM(B.click_count), 0) AS click_count,
    MIN(B.clckstrm_event_ts) AS min_clckstrm_event_ts,
    MAX(B.clckstrm_event_ts) AS max_clckstrm_event_ts

  FROM trip_info_date_bucketed A
  LEFT JOIN clickstream_agg B 
  ON 
    A.BAN = B.bus_bacct_num 
    AND B.clckstrm_event_ts >= A.start_dt AND clckstrm_event_ts < A.end_dt
  GROUP BY A.BAN, A.MSISDN, A.imsi_id, A.trip_first_date,  A.ts_index, A.start_dt, A.end_dt 
)

SELECT 
  *
FROM clickstream_table_final