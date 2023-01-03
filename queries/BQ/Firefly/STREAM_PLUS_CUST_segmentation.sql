WITH STREAM_PLUS_CUST AS(
  SELECT 
    ban AS BAN,
    subscriber_no AS MSISDN,
    1 AS has_stream_plus
  FROM `cio-datahub-enterprise-pr-183a.src_kb.bq_service_agreement` 
  WHERE soc IN ('3SOTTSTPL','SOTTSTPL','XSEOTTPL','XSOTTBPL', 'SOTTSTPL','SMHOTTPLR','SMHOTTPLP','SOTTSTR ','SOTTSTPP','SOTTSTPLR','SOTTSTPLP','SWBOTTPLR','SWBOTTPLP') -- Stream+

)

SELECT 

    A.bacct_billg_mthd_cd,
    A.bus_prod_instnc_src_id,
    A.bacct_brand_nm, -- 1 = Telus, 3 = Koodo
    A.pi_prod_instnc_typ_cd, -- Cellular 
    A.bacct_bacct_typ_cd, -- consumer vs business 
    A.bacct_bacct_typ_txt, 
    A.bacct_bacct_subtyp_cd, --subtype
    A.bacct_bacct_subtyp_txt, 
    A.bacct_bacct_stat_cd, -- status of billing account 
    A.pi_prod_instnc_stat_cd,
    
    COUNt(*) as cnt

FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` A
INNER JOIN STREAM_PLUS_CUST B
ON A.bacct_bus_bacct_num = B.BAN AND A.pi_prod_instnc_resrc_str = B.MSISDN
WHERE prod_instnc_ts = TIMESTAMP(CURRENT_DATE() - 1 )  -- Get most recent date in snapshot table
GROUP BY 1,2,3,4,5,6,7,8,9, 10