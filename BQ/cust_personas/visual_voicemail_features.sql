-- Customers who currently have Visual Voicemail Add-on
SELECT 
soc,
count(DISTINCT billg_acct_num) as count
FROM `cio-datahub-enterprise-pr-183a.ent_cust_bill.bq_wls_chrg_dtl_view` 
WHERE TRIM(UPPER(soc)) IN ('SVVM8','LSVVM8R','LSVVM8P','LSVM3MR','LSVM3MP','SSPIVVMNE','SVVMNEWR','SIVVMN','SPRVVMN','SPVVMNEW','SNNEWVVM','SPRVVMR','SSVVMR','SVVM3R','SSVISVM3R','SSPVVMR3','SSIVVMR','SVISVM3R','SVVM6MP','SVVMR5') 

GROUP BY soc 

-- CCAI dataset containing customers who called in regarding VOICEMAIL
WITH CCAI AS (
  SELECT  
    call_convrstn_dt,
    convrstn_durtn_min_qty, 
    usr_tel_num AS MSISDN,
    BUSINESS_DOMAIN,
    callType, 
    SPEECH_TOPIC,
    convrstn_transcript_txt,
    clnt_sntmnt_scor_qty,

  FROM `cio-datahub-enterprise-pr-183a.ent_cust_intractn_ccai.bq_voice_call_insights` 
  WHERE 
    call_convrstn_dt > "2022-04-01" AND
    usr_tel_num IS NOT NULL AND
    UPPER(BUSINESS_DOMAIN)='WIRELESS'  AND
    UPPER(SPEECH_TOPIC) LIKE '%VOICEMAIL%'
    
)

SELECT
*
FROM CCAI

-- Find customers who use a lot of voicemail
-- This table contains data for approx 2 months
-- This table counts the number of voice mail messages per day
WITH voicemail_cnt AS (
  SELECT 
    ptpd_voice_usg_dly_sum_dt,
    ban,
    subscr_ph_num,
    vm_rtrvl_air_call_cnt AS num_voicemail 
  FROM `cio-datahub-enterprise-pr-183a.ent_usage_rated.bq_wls_ptpd_voice_usg_dly_sum` 
  WHERE 
    ptpd_voice_usg_dly_sum_dt > "2022-05-15" 
)

SELECT
  ban,
  subscr_ph_num,
  SUM(num_voicemail) AS total_voicemail
FROM voicemail_cnt 

GROUP BY ban, subscr_ph_num
ORDER BY subscr_ph_num


SELECT
  *
FROM `cio-datahub-enterprise-pr-183a.ent_usage_rated.bq_wls_ptpd_voice_usg_dly_sum` 
WHERE subscr_ph_num='2042050036'
AND  ptpd_voice_usg_dly_sum_dt > "2022-04-15" 
ORDER BY ptpd_voice_usg_dly_sum_dt DESC

SELECT MIN(ptpd_voice_usg_dly_sum_dt)
FROM `cio-datahub-enterprise-pr-183a.ent_usage_rated.bq_wls_ptpd_voice_usg_dly_sum` 

