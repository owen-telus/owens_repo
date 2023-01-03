SELECT *
FROM `cio-datahub-enterprise-pr-183a.src_kb.bq_service_agreement` 
WHERE soc IN ('3SOTTSTPL','SOTTSTPL','XSEOTTPL','XSOTTBPL', 'SOTTSTPL',
'SMHOTTPLR',
'SMHOTTPLP',
'SOTTSTR ',
'SOTTSTPP',
'SOTTSTPLR',
'SOTTSTPLP',
'SWBOTTPLR',
'SWBOTTPLP') -- Stream+
--('41161911'	,'41161921'	,'41161931')


-- SELECT *
-- FROM `cio-datahub-enterprise-pr-183a.src_kb.bq_soc` 
-- WHERE DATE(snapshot_load_ts) = "2022-12-01"
-- AND soc in ('3SOTTSTPL','SOTTSTPL','XSEOTTPL','XSOTTBPL', 'SOTTSTPL',
-- 'SMHOTTPLR',
-- 'SMHOTTPLP',
-- 'SOTTSTR ',
-- 'SOTTSTPP',
-- 'SOTTSTPLR',
-- 'SOTTSTPLP',
-- 'SWBOTTPLR',
-- 'SWBOTTPLP')


--   SELECT 
--     A.bacct_bus_bacct_num AS BAN,
--     A.pi_prod_instnc_resrc_str AS MSISDN,
   
--   FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` A

--   WHERE 
--     prod_instnc_ts = TIMESTAMP(CURRENT_DATE() - 1 )  -- Get most recent date in snapshot table
--     AND bacct_brand_id=1 -- 1 For Telus
--     AND pi_prod_instnc_typ_cd = 'C'-- Celluluar products 
--     AND bacct_bacct_typ_cd = 'I' -- Consumer
--     AND bacct_bacct_subtyp_cd = 'R' -- Account Sub type
--     AND bacct_bacct_stat_cd = 'O' -- Billing account open  
--     AND pi_prod_instnc_stat_cd = 'A' -- Status of product instance
--     AND bacct_billg_mthd_cd ='POST' -- Post Pay customers only
--     AND A.bus_prod_instnc_src_id = 130
--     AND pp_bus_pp_catlg_itm_cd IN ('3SOTTSTPL','SOTTSTPL','XSEOTTPL','XSOTTBPL', 'SOTTSTPL',
-- 'SMHOTTPLR',
-- 'SMHOTTPLP',
-- 'SOTTSTR ',
-- 'SOTTSTPP',
-- 'SOTTSTPLR',
-- 'SOTTSTPLP',
-- 'SWBOTTPLR',
-- 'SWBOTTPLP')
   
    --AND B.prod_instnc_alias_str = '302220023511271'