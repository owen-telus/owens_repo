

-- call_to_tn_txt is the destination phone number
-- subscriber_num	is the source phone number aka MSISDN
SELECT subscriber_num, call_to_tn_txt FROM `cio-datahub-enterprise-pr-183a.src_kb.bq_wls_ptpd_unbilld_dly_call_usg_evnt_dtl` 
WHERE 
  DATE(channel_seizure_dt) >= "2022-06-01" AND 
  call_to_tn_txt = '*611'
LIMIT 1000

-- Top 5 results are telus service numbers
with telus_service_numbers AS (
  SELECT call_to_city_desc_str, call_to_tn_txt, count(*)
  FROM `cio-datahub-enterprise-pr-183a.src_kb.bq_wls_ptpd_unbilld_dly_call_usg_evnt_dtl` 
  WHERE DATE(channel_seizure_dt) >= "2022-05-01" 
  AND UPPER(call_to_city_desc_str) LIKE '%TELUS%'
  GROUP BY call_to_city_desc_str, call_to_tn_txt
  ORDER BY count(*) DESC
  LIMIT 5
)

SELECT 
  A.subscriber_num,
  COUNT(*) as num_calls
FROM `cio-datahub-enterprise-pr-183a.src_kb.bq_wls_ptpd_unbilld_dly_call_usg_evnt_dtl` A
WHERE A.call_to_tn_txt IN (SELECT call_to_tn_txt FROM telus_service_numbers)
AND DATE(channel_seizure_dt) >= "2022-05-01"-- Change Date
GROUP BY A.subscriber_num

-- Top counts are telus service phone numbers
SELECT 
  subscriber_num,
  call_to_city_desc_str,
  call_to_tn_txt,
  COUNT(*) as num_calls
FROM `cio-datahub-enterprise-pr-183a.src_kb.bq_wls_ptpd_unbilld_dly_call_usg_evnt_dtl` 
WHERE 
  DATE(channel_seizure_dt) >= "2022-01-24" AND -- DATE(channel_seizure_dt) <= "2022-04-23" AND 
  UPPER(call_to_city_desc_str) LIKE '%TELUS%'
GROUP BY subscriber_num,call_to_city_desc_str, call_to_tn_txt
ORDER BY COUNT(*) DESC

SELECT MIN(channel_seizure_dt), MAX(channel_seizure_dt) FROM `cio-datahub-enterprise-pr-183a.src_kb.bq_wls_ptpd_unbilld_dly_call_usg_evnt_dtl` 

-- bq_cstm_tkt_ticket


-- CCAI dataset, usr_tel_num is MSISDN
-- SPEECH_TOPIC most of the time is null
-- CCAI dataset

-- following query, join on MSISDN to get number of calls and avg call duration of customers who called into customer care
-- following dataset that Wei copied for me date range Jan 24 - April 28
SELECT 
  usr_tel_num, 
  COUNT(*) as num_calls,
  AVG(convrstn_durtn_min_qty) as avg_convrstn_durtn_min
FROM `roaming-pr-66a1b0.ent_cust_intractn_ccai.bq_voice_call_insights` 
WHERE usr_tel_num is not null AND callType = 'Inbound'
GROUP BY usr_tel_num
ORDER BY COUNT(*) DESC


SELECT *
FROM `roaming-pr-66a1b0.ent_cust_intractn_ccai.bq_voice_call_insights` 
WHERE usr_tel_num = '5148835308' --  '6043103100'

SELECT MIN(call_convrstn_dt), MAX(call_convrstn_dt)
 FROM `roaming-pr-66a1b0.ent_cust_intractn_ccai.bq_voice_call_insights` 
