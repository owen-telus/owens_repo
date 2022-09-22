-- Exploring the wls Billing tables

-- bq_wls_pymt_dtl_view
-- contains payment, ent_seq_no is the 'time stamp / bill date'
-- at BAN level
SELECT * FROM `cio-datahub-enterprise-pr-183a.ent_cust_bill.bq_wls_pymt_dtl_view` 
WHERE bacct = 36716759
ORDER BY ent_seq_no

--bq_wls_bill_subsc_dtl_view
-- contains wireless bill information at a subscriber phone level
-- bill seq num is the 'time stamp / bill date'
-- bill seq num is at ban level. Ie bill_seq_num = 1 for first bill for all subscribers in one ban, = 2 for second month, same for all BAN's
SELECT * FROM `cio-datahub-enterprise-pr-183a.ent_cust_bill.bq_wls_bill_subsc_dtl_view`
WHERE bacct_num = 36716759
ORDER BY bill_seq_num


-- is src_id the method of payment?
SELECT
  src_id,
  COUNT(*)
FROM `cio-datahub-enterprise-pr-183a.ent_cust_bill.bq_wls_pymt_dtl_view` 
GROUP BY src_id

-- Has late payment columns, total amount due
SELECT * FROM `cio-datahub-enterprise-pr-183a.ent_cust_bill.bq_wls_bill_dtl_view` 
WHERE billg_acct_num = 39315910

-- contains history information on the Automatic and Manual Collection activities
SELECT * FROM `cio-datahub-enterprise-pr-183a.ent_cust_bill.bq_wls_coll_dtl_view` 
WHERE billg_acct_num=39315910


SELECT
bank_brnch_num,
count(*)
FROM `cio-datahub-enterprise-pr-183a.ent_cust_bill.bq_wls_pymt_dtl_view` 
GROUP BY bank_brnch_num

SELECT * FROM `cio-datahub-enterprise-pr-183a.ent_cust_bill.bq_wls_pymt_dtl_view` 
WHERE bacct = 39315910


SELECT 
*
FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` 
WHERE 
prod_instnc_ts = (SELECT MAX(prod_instnc_ts) FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` )  -- Get most recent date in snapshot table
AND BACCT_BUS_BACCT_NUM = 39315910


-- bacct_ebill_ind = Paper or e-bill

SELECT 
bacct_auto_pymt_mthd_cd,
count(*)

FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` 
WHERE 
prod_instnc_ts = (SELECT MAX(prod_instnc_ts) FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` )  -- Get most recent date in snapshot table
GROUP BY bacct_auto_pymt_mthd_cd

SELECT  
actv_cd,
actv_rsn_cd,
count(*)
FROM `cio-datahub-enterprise-pr-183a.ent_cust_bill.bq_wls_pymt_actv_dtl_view` 
GROUP BY actv_cd, actv_rsn_cd

SELECT  
*
FROM `cio-datahub-enterprise-pr-183a.ent_cust_bill.bq_wls_pymt_actv_dtl_view` 
WHERE billg_acct_num = 39315910