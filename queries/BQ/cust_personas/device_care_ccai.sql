-- Get Identifiers, Call Duration, Business Domain, and Speech Topic
WITH CCAI AS (
  SELECT  
    call_convrstn_dt,
    convrstn_durtn_min_qty, 
    usr_tel_num AS MSISDN,
    BUSINESS_DOMAIN,
    callType, 
    SPEECH_TOPIC,
    clnt_sntmnt_scor_qty,

  FROM `cio-datahub-enterprise-pr-183a.ent_cust_intractn_ccai.bq_voice_call_insights` 
  WHERE 
    call_convrstn_dt > "2022-04-01" AND
    usr_tel_num IS NOT NULL AND
    UPPER(BUSINESS_DOMAIN)='WIRELESS' 
    AND
    
    SPEECH_TOPIC IN ('REPAIR_PHONE', 'INQUIRE_DEVICE_PROTECTION', 'REPORT_LOST_STOLEN_PHONE',
                    'REQUEST_TECH_SUPPORT', 'UNLOCK_DEVICE', 'REPORT_PROBLEM_PHONE')
)


SELECT * 
FROM CCAI


-- SELECT 
--   BUSINESS_DOMAIN,
--   SPEECH_TOPIC,
--   COUNT(*)
-- FROM CCAI 
-- WHERE 
--   UPPER(BUSINESS_DOMAIN)='WIRELESS' 
--   AND
--   SPEECH_TOPIC IN ('REPAIR_PHONE', 'INQUIRE_DEVICE_PROTECTION', 'REPORT_LOST_STOLEN_PHONE',
--                   'REQUEST_TECH_SUPPORT', 'UNLOCK_DEVICE', 'CONFIRM_MAX_ATTEMPTS', 
--                   'REPORT_PROBLEM_PHONE', 'ACTIVATE_PHONE')

-- GROUP BY BUSINESS_DOMAIN, SPEECH_TOPIC
-- ORDER BY COUNT(*) DESC