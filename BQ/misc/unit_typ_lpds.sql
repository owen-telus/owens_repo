-- Get Unit Type based on ldps_address_id and customer.
-- Options are BLDG, APT, SWIFT
WITH UNIT_TYPE AS (
  SELECT 
    t1.unit_typ_cd,
    t1.lpds_addr_id,
    t2.cust_id
  FROM `cio-datahub-enterprise-pr-183a.ent_resrc_config.bq_service_address_daily_snpsht` t1
  INNER JOIN `cto-wln-sa-data-pr-bb5283.ref_table.bq_datalake_prem_profile` t2 
  ON t1.lpds_addr_id=CAST(t2.lpds_id AS STRING)

  WHERE part_create_dt = (SELECT MAX(part_create_dt) FROM  `cio-datahub-enterprise-pr-183a.ent_resrc_config.bq_service_address_daily_snpsht` )

)

SELECT 
  unit_typ_cd,
  COUNT(*)
FROM UNIT_TYPE
GROUP BY unit_typ_cd 