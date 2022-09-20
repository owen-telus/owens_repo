-- SELECT 
--   DISTINCT category1_txt, category2_txt, category3_txt
-- FROM `cio-datahub-enterprise-pr-183a.ent_trouble_ticket.bq_cstm_tkt_ticket` 
-- WHERE DATE(src_create_dt_ts) >= "2022-01-27" 
-- AND (UPPER(category1_txt) LIKE '%ROAM%' OR 
--      UPPER(category2_txt) LIKE '%ROAM%' OR 
--      UPPER(category3_txt) LIKE '%ROAM%' )

-- only 1 ticket in following query
-- SELECT *
-- FROM `cio-datahub-enterprise-pr-183a.ent_trouble_ticket.bq_cstm_tkt_ticket` 
-- WHERE DATE(src_create_dt_ts) >= "2022-01-27" AND
--       UPPER(category2_txt)  = 'OUTBOUND ROAMER'

SELECT 
  category1_txt, category2_txt, category3_txt,
  COUNT(*) as num_calls
FROM `cio-datahub-enterprise-pr-183a.ent_trouble_ticket.bq_cstm_tkt_ticket` 
WHERE DATE(src_create_dt_ts) >= "2022-01-27" 
AND (UPPER(category1_txt) LIKE '%ROAM%' OR 
     UPPER(category2_txt) LIKE '%ROAM%' OR 
     UPPER(category3_txt) LIKE '%ROAM%' )
GROUP BY category1_txt, category2_txt, category3_txt



-- SELECT DISTINCT category1_cd, category2_cd, category3_cd 
-- FROM `cio-datahub-enterprise-pr-183a.ent_trouble_ticket.bq_ticket`
-- WHERE DATE(src_create_ts) >= "2022-01-27"
-- AND (UPPER(category1_cd) LIKE '%ROAM%' OR 
--      UPPER(category2_cd) LIKE '%ROAM%' OR 
--      UPPER(category3_cd) LIKE '%ROAM%' )