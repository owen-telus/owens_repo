-- phone number, date, word_flag from chatbot
SELECT
  --chatbot_sess_str AS sess_id,
  chatbot_user_ph_num AS msisdn,
  chatbot_intractn_part_dt AS part_dt,
  SUM(IF(REGEXP_CONTAINS(LOWER(chatbot_msg_txt), r'travel|roaming|international travel|suspension|roaming charge|easy roam|vacation disconnect|trip|weekend trip'), 1, 0)) AS word_flag
FROM
  `cio-datahub-enterprise-pr-183a.ent_chat.bq_chatbot_intractn`
WHERE
  --chatbot_intractn_part_dt BETWEEN '2022-02-15' AND '2022-04-26'
  --AND chatbot_intractn_part_dt >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)
  chatbot_ivr_desc IN ('KoodoAssistPostpaid', 'MOBTVA', 'MobPrepaidTVA', 'MyTELUSApp', 'TBSWLScustomerfacing', 'KoodoPrepaid', 'PUBLICMOBILE', 'TELUSSalesMOBFFH', 'TBSWLSHD_IQ', 'KoodoFVA', 'MOB')
GROUP BY chatbot_user_ph_num, chatbot_intractn_part_dt
