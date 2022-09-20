-- Returns BAN, MSISDN, IMSI, and date account switched to Suspension
SELECT 
  BAN,
  MSISDN,
  imsi_id,
  prod_instnc_ts
  
  FROM (
    SELECT
      A.bap_bus_bacct_num AS BAN,
      A.subscriber_num AS MSISDN,
      A.imsi_id,
      B.prod_instnc_ts,
      --B.pi_prod_instnc_stat_cd,
      ROW_NUMBER() OVER(PARTITION BY A.bap_bus_bacct_num, A.subscriber_num, A.imsi_id, B.pi_prod_instnc_stat_cd ORDER BY B.prod_instnc_ts ASC) AS ROW_NUM
      --count(distinct DATE(B.prod_instnc_ts)) as total_suspension_flag_count
    FROM `roaming-pr-66a1b0.pre_trip_modeling_v2.pre_trip_labels_final` A
    LEFT JOIN  `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` B
    ON A.bap_bus_bacct_num = B.bacct_bus_bacct_num AND A.subscriber_num = B.pi_prod_instnc_resrc_str
    WHERE 
      B.bacct_billg_mthd_cd = 'POST'
      AND B.bus_prod_instnc_src_id = 130
      AND B.pi_prod_instnc_typ_cd = 'C'
      AND B.pi_prod_instnc_resrc_typ_cd = 'TN' -- ?
      AND B.bacct_brand_id IN (1,3)
      AND B.pi_prod_instnc_stat_cd = 'S'
    --GROUP BY A.bap_bus_bacct_num, A.subscriber_num, A.imsi_id
    ORDER BY  A.bap_bus_bacct_num, A.subscriber_num, A.imsi_id, B.prod_instnc_ts
  )
WHERE ROW_NUM = 1