-- This query gets the date of a travel click
-- See how I want to join this later in Python during EDA
SELECT
  A.bap_bus_bacct_num AS BAN,
  A.subscriber_num AS MSISDN,
  A.imsi_id,
  C.clckstrm_event_ts,
  COUNT(DISTINCT C.clckstrm_event_id) AS click_count
FROM `roaming-pr-66a1b0.pre_trip_modeling_v2.pre_trip_labels_final` A
LEFT JOIN `cio-datahub-enterprise-pr-183a.ent_party_identity.bq_cust_idnty_billg_acct_snpsht` B
ON A.bap_bus_bacct_num = B.bus_bacct_num
LEFT JOIN `cio-datahub-enterprise-pr-183a.ent_cust_intractn.bq_clckstrm_telus_web_event` C
ON B.uuid = C.uuid
WHERE 
  (pg_hit_url_str LIKE '%/mobility%' AND pg_nm LIKE '%travel%' AND pg_nm NOT LIKE '%business%')
  AND B.cust_idnty_billg_acct_ts = (SELECT MAX(cust_idnty_billg_acct_ts) FROM `cio-datahub-enterprise-pr-183a.ent_party_identity.bq_cust_idnty_billg_acct_snpsht`)

GROUP BY A.bap_bus_bacct_num, A.subscriber_num, A.imsi_id, C.clckstrm_event_ts