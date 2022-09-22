-- usr_tel_num

-- SELECT 
-- MIN(call_convrstn_dt),
-- MAX(call_convrstn_dt)
--  FROM `roaming-pr-66a1b0.ent_cust_intractn_ccai.bq_voice_call_insights` 

-- convrstn_entity.entity_sntmnt_scor_qty
-- FROM `roaming-pr-66a1b0.ent_cust_intractn_ccai.bq_voice_call_insights` , UNNEST(convrstn_entity) AS convrstn_entity
WITH USA_roamer_usage_data AS (

  SELECT 
    MAX(event_dt) as most_recent_date,
    imsi_num,       
    --app_category_nm,
    SUM(dl_volume_qty/1000000.0) as dl_usage_mb --Get total usage by each imsi
  FROM
    `cio-datahub-enterprise-pr-183a.ent_usage_unrated_ott.bq_ott_app_event`
  WHERE 
    event_dt between "2022-03-01" AND "2022-04-20" 
    AND imsi_num like '302220%'
    AND (mcc_id = '311' OR mcc_id = '310') -- get customers who roamed

  GROUP BY imsi_num -- , app_category_nm
),

sentiment_score AS (
  SELECT
    call_convrstn_id,
    converstn.entity_sntmnt_scor_qty as sentiment_score
  FROM 
    `roaming-pr-66a1b0.ent_cust_intractn_ccai.bq_voice_call_insights`,  UNNEST(convrstn_entity) as converstn
)

SELECT 
  A.*,
  B.MSISDN,
  B.MOB_BAN AS BAN,
  C.call_convrstn_id,
  C.call_convrstn_dt,
  C.tot_durtn_min_qty, 
  C.convrstn_transcript_txt,
  C.SPEECH_TOPIC,
  C.convrstn_entity,
  D.sentiment_score
FROM USA_roamer_usage_data A  
LEFT JOIN `cto-wln-sa-data-pr-bb5283.customer_personas_features.cust_wls_mnh_mapping_TB` B  
ON A.imsi_num = B.IMSI
INNER JOIN `roaming-pr-66a1b0.ent_cust_intractn_ccai.bq_voice_call_insights` C -- Data from Mar 1 to April 20
ON B.MSISDN = C.usr_tel_num
LEFT JOIN sentiment_score D
ON C.call_convrstn_id = D.call_convrstn_id
WHERE C.call_convrstn_id IS NOT NULL