
-- FROM EDA, I see a trend where customers who have chatbot  events are travelling
-- Create Table with BAN, MSISDN, IMSI, ts_index (Which essentially is the time step and chatbot Event
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

chatbot_agg AS (

  SELECT
  --chatbot_sess_str AS sess_id,
    chatbot_user_ph_num AS msisdn,
    DATE(chatbot_intractn_part_dt) AS part_dt,
    SUM(IF(REGEXP_CONTAINS(LOWER(chatbot_msg_txt), r'travel|roaming|international travel|suspension|roaming charge|easy roam|vacation disconnect|trip|weekend trip'), 1, 0)) AS word_flag
  FROM
    `cio-datahub-enterprise-pr-183a.ent_chat.bq_chatbot_intractn`
  WHERE
    chatbot_ivr_desc IN ('KoodoAssistPostpaid', 'MOBTVA', 'MobPrepaidTVA', 'MyTELUSApp', 'TBSWLScustomerfacing', 'KoodoPrepaid', 'PUBLICMOBILE', 'TELUSSalesMOBFFH', 'TBSWLSHD_IQ', 'KoodoFVA', 'MOB')
  GROUP BY chatbot_user_ph_num, chatbot_intractn_part_dt 
),

chatbot_table_final AS (

  SELECT
    A.BAN,
    A.MSISDN,
    A.imsi_id,
    A.trip_first_date,
    A.ts_index,
    A.start_dt,
    A.end_dt,
    IFNULL(SUM(B.word_flag), 0) AS word_flag_cnt,
    MIN(B.part_dt) AS min_part_dt,
    MAX(B.part_dt) AS max_part_dt
    
    
  FROM trip_info_date_bucketed A
  LEFT JOIN chatbot_agg B 
  ON 
    A.MSISDN = B.msisdn 
    AND B.part_dt >= A.start_dt AND B.part_dt < A.end_dt 
  -- Need this GROUP BY in the case where there are multiple rows for a date bucket from chatbot_agg -> Remove duplicates after joining
  GROUP BY A.BAN, A.MSISDN, A.imsi_id, A.trip_first_date,  A.ts_index, A.start_dt, A.end_dt 
  --ORDER BY MSISDN, ts_index
)


SELECT
*
FROM chatbot_table_final
--WHERE imsi_id = '302220038026916'
