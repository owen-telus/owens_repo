WITH MOB_POSTPAID_BASE AS(

  SELECT 
    A.bacct_bus_bacct_num AS BAN,
    A.pi_prod_instnc_resrc_str AS MSISDN,
    B.prod_instnc_alias_str AS IMSI,
    A.bus_prod_instnc_src_id,
    A.bus_prod_instnc_id 
  FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` A
  INNER JOIN `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_alias_snpsht` B
  ON A.bus_prod_instnc_id = B.bus_prod_instnc_id 
  AND A.bus_prod_instnc_src_id = B.bus_prod_instnc_src_id
  WHERE 
    prod_instnc_ts = TIMESTAMP(CURRENT_DATE() - 1 )  -- Get most recent date in snapshot table
    AND prod_instnc_alias_ts = TIMESTAMP(CURRENT_DATE() - 1)
    AND bacct_brand_id=1 -- 1 For Telus
    AND pi_prod_instnc_typ_cd = 'C'-- Celluluar products 
    AND bacct_bacct_typ_cd = 'I' -- Consumer
    AND bacct_bacct_subtyp_cd = 'R' -- Account Sub type
    AND bacct_bacct_stat_cd = 'O' -- Billing account open  
    AND pi_prod_instnc_stat_cd = 'A' -- Status of product instance
    AND bacct_billg_mthd_cd ='POST' -- Post Pay customers only
    AND prod_instnc_alias_typ_cd='IMSI'
    AND A.bus_prod_instnc_src_id = 130
    AND B.bus_prod_instnc_src_id = 130 

),

-- IMSI_TAC AS (
--   SELECT
--     A.IMSI,
--     B.event_dt,
--     B.imei_tac_id AS TAC_ID
--   FROM MOB_POSTPAID_BASE A 
--   LEFT JOIN `cio-datahub-enterprise-pr-183a.ent_usage_unrated_ott.bq_ott_app_event` B 
--   ON A.IMSI = B.imsi_num 
--   WHERE event_dt = '2022-12-15'
-- ), 

-- unique_devices_by_imsi AS (
--   SELECT
--     A.IMSI,
--     COUNT(DISTINCT TAC_ID) AS cnt_distinct_tac 
--   FROM IMSI_TAC A
--   GROUP BY A.IMSI 
--   ORDER BY 2 DESC 
-- )

-- SELECT 
--   imsi_num, 
--   device_model_nm
-- FROM `cio-datahub-enterprise-pr-183a.ent_usage_unrated_ott.bq_ott_app_event` 
-- WHERE imsi_num IN (SELECT IMSI FROM unique_devices_by_imsi) AND  event_dt = '2022-12-15'
-- AND apn_nm LIKE 'isp%'


SUBSCRIBER_TAC AS (
  SELECT
    A.*,
    B.prod_equip_ser_num,
    SUBSTR(B.prod_equip_ser_num, 0, 8) AS TAC_ID
  FROM MOB_POSTPAID_BASE A 
  LEFT JOIN `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_equip_assn_view` B 
  ON A.bus_prod_instnc_src_id = B.bus_prod_instnc_src_id
  AND SAFE_CAST(A.bus_prod_instnc_id AS INT64) = B.bus_prod_instnc_id
  WHERE B.most_rcnt_ind = 'Y' AND LENGTH(B.prod_equip_ser_num) < 19

),

DEVICE_INFO AS (
  SELECT 
    A.*,
    B.type_txt,
    B.manufacturer_nm,
    B.techno_txt, 
    B.market_typ,
    B.proj_launch_dt1,
    C.is_3500_capable,
    ROW_NUMBER() OVER(PARTITION BY A.BAN, A.MSISDN, A.IMSI ORDER BY B.proj_launch_dt1 DESC) AS device_launch_dt_row_num
  FROM SUBSCRIBER_TAC A 
  LEFT JOIN `cio-datahub-enterprise-pr-183a.ent_prod_offr.bq_tac_device_type_alloc_code` B 
  ON A.TAC_ID = B.tac_id 
  LEFT JOIN `roaming-pr-66a1b0.5g_upsell.bq_5g_device_tac` C 
  ON A.TAC_ID = C.tac_id 
  WHERE B.Snapshot_load_dt = TIMESTAMP(CURRENT_DATE() - 1 ) 
)


SELECT 
  *
FROM DEVICE_INFO A 
WHERE 
 device_launch_dt_row_num = 1

