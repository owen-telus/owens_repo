-- speed tiers target list
-- 60k customers in target group, 6668 customers in control group

WITH cust_list AS (
  SELECT 
    B.IMSI,
    A.BAN,
    A.SUBSCRIBER_NO AS MSISDN,
    A.CONTROL_GROUP
  FROM (
    SELECT * FROM `cto-wln-sa-data-pr-bb5283.temp_workspace.speedtiers_target_list_v1` 
    UNION ALL
    SELECT * FROM `cto-wln-sa-data-pr-bb5283.temp_workspace.speedtiers_target_list_v2`) A
  LEFT JOIN `cto-wln-sa-data-pr-bb5283.customer_personas_features.cust_wls_mnh_mapping_TB` B 
  ON A.BAN = B.MOB_BAN AND CAST(A.SUBSCRIBER_NO AS STRING)=B.MSISDN
),

cust_5g AS (
  SELECT 
    pi_prod_instnc_resrc_str AS MSISDN,
    BACCT_BUS_BACCT_NUM AS BAN,  
    pp_bus_pp_catlg_itm_cd , 

  FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` 
  WHERE 
    prod_instnc_ts = (SELECT MAX(prod_instnc_ts) FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust.bq_prod_instnc_snpsht` ) AND -- Get most recent date in snapshot table
    pp_bus_pp_catlg_itm_cd IN ('PPM25G85','PPM40G90','POM50G100','PMUL5GB70','PMUL10GB','PMUL15GB','PES6GUL60','PULENW6GB','PES15G65N','PUL30G85','PPM20G80','PPM20G75U','PPM25G75N','PPM25G80P','PPM40G80U','POM40G85N','PPM45P90N','POM50G90N','PPM40G85P','POM50G95N','PPM10G4G','PULNWS30G','PUL35NW85','PMUL20Q70','PUL20G70','PPM40G80Q','PPM40G80N','PPM50G90Q','POM50G90','PES12GBQC','PES12GBON','PES15GBMS','PPM15MS65','PPM25QC60','PPM25ON60','PPM25MS65','PPM45MS65','PULSH20MS','PPM20MS70','PSKUNW20G','PESS20G65','PESS20GON','PMUL45G65','PMUL45GPP','PMUL25Q65','PMUL25G65','PPM45MS70','PMUL25GSM','PPMQ45G70','PPMO45G70','PPM45G70','POM45G70','PPM40QC75','PPM40MS75','PPMS45G75','PPM40GQ75','PPM40ON75','PMUL45GQC','PSKU45G75','PPM45MS75','PMUL45GON','PPMB20G80','POM45G75','PPM45G75','PULSH80','PPM40MS80','PSKUNW40G','PMUL40GPN','PMUL45GSM','PPM40GQ80','PPM40GO80','PPM50G80','PPM50GQ80','PSKU50G80','POM50SKMB','POM50G80','PPM60MS80','POM50ON80','PPM60GQ80','PPMS50GCU','PPMQ50GCU','PPMO50GCU','PSKUN50GP','PPMB40G90','PPM50G85','POM50G85','PPM50GQCU','PPM50GOCU','POMUU100P','PSKUNW50G','PPM60GQ90','PPM60GO90','PPM60MS90','PPMB50GCU')
),


new_cust_5g AS (
  SELECT
    A.*,
    B.pp_bus_pp_catlg_itm_cd,
    CASE WHEN B.pp_bus_pp_catlg_itm_cd IS NOT NULL THEN 1 ELSE 0 END AS label -- 1 means customer adopted, 0 means customer did not adopt
  FROM cust_list A 
  LEFT JOIN cust_5g B 
  ON CAST(A.MSISDN AS STRING) = B.MSISDN AND A.BAN=B.BAN 

),

lift AS (
  SELECT
    A.target_num,
    A.target_denom,
    CAST(A.target_num AS DECIMAL) / CAST(A.target_denom AS DECIMAL) AS target_lift,
    A.control_num,
    A.control_denom,
    CAST(A.control_num AS DECIMAL) / CAST(A.control_denom AS DECIMAL) AS control_lift
  FROM(
      SELECT 
        (SELECT COUNT(*) FROM new_cust_5g WHERE CONTROL_GROUP=FALSE AND label=1) AS target_num,
        (SELECT COUNT(*) FROM new_cust_5g WHERE CONTROL_GROUP=TRUE AND label=1) AS control_num,
        (SELECT COUNT(*) FROM cust_list WHERE CONTROL_GROUP=FALSE) AS target_denom,
        (SELECT COUNT(*) FROM cust_list WHERE CONTROL_GROUP=TRUE) AS control_denom
 
      ) A
  )

SELECT
   BAN, CAST(MSISDN AS STRING) AS MSISDN, IMSI, CONTROL_GROUP, label
FROM new_cust_5g
-- SELECT * FROM lift