-- Queries to check various tables for SOC Codes

-- Product Instance Tables
SELECT 
  pp_bus_pp_catlg_itm_cd,
 COUNT(*)
FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` 
WHERE 
  prod_instnc_ts = (SELECT MAX(prod_instnc_ts) FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` ) AND -- Get most recent date in snapshot table
  pp_bus_pp_catlg_itm_cd IN ('40797411', '40797421' , '40797431' , '40797441' , '40797811' , '41083751' , '40800491', '41086361', '41093531', '41093541' , '41161911', '41161921', '41161931' ) 
GROUP BY pp_bus_pp_catlg_itm_cd

-- Not in this table
SELECT 
  PRIM_PRICE_PLAN_CD,
  COUNT(*) 
FROM `cto-wln-sa-data-pr-bb5283.ref_table.bq_hpbi_product_instance_profl`
WHERE PRIM_PRICE_PLAN_CD IN  ('40797411', '40797421' , '40797431' , '40797441' , '40797811' , '41083751' , '40800491', '41086361', '41093531', '41093541' , '41161911', '41161921', '41161931' )

-- Wireless Billing Table
SELECT 
soc,
count(*) as count
FROM `cio-datahub-enterprise-pr-183a.ent_cust_bill.bq_wls_chrg_dtl_view` 
WHERE TRIM(UPPER(soc)) IN  ('40797411', '40797421' , '40797431' , '40797441' , '40797811' , '41083751' , '40800491', '41086361', '41093531', '41093541' , '41161911', '41161921', '41161931' ) 
GROUP BY soc 

-- Wireline Billing Table
SELECT 
  prc_plan_cd, 
  COUNT(*)
FROM `cio-datahub-enterprise-pr-183a.ent_cust_bill.bq_wln_chrg_dtl` 
WHERE 
  bill_dt = (SELECT MAX(bill_dt) FROM `cio-datahub-enterprise-pr-183a.ent_cust_bill.bq_wln_chrg_dtl` ) 
  AND prc_plan_cd IN  ('40797411', '40797421' , '40797431' , '40797441' , '40797811' , '41083751' , '40800491', '41086361', '41093531', '41093541' , '41161911', '41161921', '41161931' ) 

GROUP BY prc_plan_cd 

-- join bill_doc_id from following table with bq_wln_document_dtl
SELECT 
  *
FROM `cio-datahub-enterprise-pr-183a.ent_cust_bill.bq_wln_chrg_dtl` 
WHERE 
  bill_dt = (SELECT MAX(bill_dt) FROM `cio-datahub-enterprise-pr-183a.ent_cust_bill.bq_wln_chrg_dtl` ) 
  AND prc_plan_cd IN  ('40797411', '40797421' , '40797431' , '40797441' , '40797811' , '41083751' , '40800491', '41086361', '41093531', '41093541' , '41161911', '41161921', '41161931' ) 


-- HPBI Price Plan Dimension Table
SELECT 
PRICE_PLAN_CD,
PRICE_PLAN_TXT,
COUNT(*)
FROM `cto-wln-sa-data-pr-bb5283.ref_table.bq_hpbi_price_plan_dim` 
WHERE PRICE_PLAN_CD IN  ('40797411', '40797421' , '40797431' , '40797441' , '40797811' , '41083751' , '40800491', '41086361', '41093531', '41093541' , '41161911', '41161921', '41161931' )
GROUP BY PRICE_PLAN_CD, PRICE_PLAN_TXT 


