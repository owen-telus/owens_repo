-- Identify Accounts with LWC Products by BAN
-- About 55k accounts have LWC Products
-- These are all FFH BAN's with bus_bacct_num_src_id = 1001
WITH LWC_ACCOUNTS AS (
  SELECT 
    bus_prod_instnc_id,
    bus_bacct_num_src_id,
    bus_bacct_num AS FFH_BAN ,
    pi_prod_instnc_stat_cd, -- Status of the product instance - A = Active, O = open, C = Cancelled
    pi_prim_pp_sls_start_ts AS product_start_ts,
    pi_cntrct_start_ts, 
    pi_cntrct_end_ts,
    pi_cntrct_durtn_mth_qty,
    bus_pp_catlg_itm_cd, 
    bus_pp_catlg_itm_nm
  FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_subscn_item_profl_snpsht` 
  WHERE snpsht_dt = (SELECT MAX(snpsht_dt) FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_subscn_item_profl_snpsht` )
  AND bus_pp_catlg_itm_cd IN  ('40797411', '40797421' , '40797431' , '40797441' , '40797811' , '41083751' , '40800491', '41086361', '41093531', '41093541' , '41161911', '41161921', '41161931' )
),

HOUSEHOLD_SERVICES AS (
  SELECT
    bus_bacct_num,
    hsehld_src_id,
    hsehld_actv_party_cnt,
    hsehld_actv_hsic_srvc_cnt,
    hsehld_actv_dial_up_srvc_cnt,	
    hsehld_actv_hm_ph_srvc_cnt,
    hsehld_actv_smart_hm_srvc_cnt,
    hsehld_actv_post_mbl_srvc_cnt,
    hsehld_actv_pre_mbl_srvc_cnt,
    hsehld_actv_optc_tv_srvc_cnt,
    hsehld_actv_sat_tv_srvc_cnt,
    hsehld_actv_oth_srvc_cnt,	
    hsehld_canc_bacct_cnt,
    hsehld_suspnd_bacct_cnt,
    hsehld_actv_bacct_cnt,
    hsehld_actv_wls_srvc_only_ind,
    hsehld_actv_wln_srvc_only_ind,
    hsehld_actv_wln_srvc_cnt,
    hsehld_actv_wls_koodo_srvc_cnt,
    hsehld_actv_wls_telus_srvc_cnt
  FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_hsehld_bacct_dly_snpsht` 
  WHERE DATE(hsehld_bacct_dly_ts) = (SELECT MAX(DATE(hsehld_bacct_dly_ts)) FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_hsehld_bacct_dly_snpsht` )
)


SELECT
  COUNT(DISTINCT FFH_BAN)
FROM LWC_ACCOUNTS


-- See how many accounts have LWC products from the wireline billing table
-- join bill_doc_id from following table with bq_wln_document_dtl
-- Billing table shows 1020 distinct billing accounts ... 
-- WITH LWC_bill_doc_id AS (
--   SELECT 
--     bill_doc_id
--   FROM `cio-datahub-enterprise-pr-183a.ent_cust_bill.bq_wln_chrg_dtl` 
--   WHERE 
--     bill_dt = (SELECT MAX(bill_dt) FROM `cio-datahub-enterprise-pr-183a.ent_cust_bill.bq_wln_chrg_dtl` ) 
--     AND prc_plan_cd IN  ('40797411', '40797421' , '40797431' , '40797441' , '40797811' , '41083751' , '40800491', '41086361', '41093531', '41093541' , '41161911', '41161921', '41161931' ) 
-- )

-- SELECT
--   COUNT(DISTINCT billg_acct_num)
-- FROM `cio-datahub-enterprise-pr-183a.ent_cust_bill.bq_wln_document_dtl` 
-- WHERE bill_doc_id IN (SELECT bill_doc_id FROM LWC_bill_doc_id)


-- -- Product summary at BAN Level:
-- SELECT * FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_hsehld_bacct_dly_snpsht` WHERE DATE(hsehld_bacct_dly_ts) = "2022-06-16" AND bus_bacct_num=604169589