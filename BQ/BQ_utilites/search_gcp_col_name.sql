-- Get Table Name, Column Name, Data Type, Description of most tables within cio-datahub-enterprise-pr-183a. 
-- Doesn't include tables that require additional access

WITH table_name_columns AS (

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.src_ngsp`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.ent_trouble_ticket`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.src_dist`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.src_mastermind`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.src_enabler`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.ent_cust_ord`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.src_outcome`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust_ess`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.src_rrem`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.ent_usage_rated`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.ent_cust_ord_wln`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.ent_resrc_performance`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.src_pods`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.ent_cust_ord_ess`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.src_custods`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.ent_party_credit_profl`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust_tos`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.src_iwd`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.ent_resrc_resrc`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.src_contact_ai`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.ent_usage_unrated`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.ent_chat`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.ent_prod_offr`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL


  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust_rwrd`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.int_cust_intractn`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust_pref`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.src_oms`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.int_cust_cust`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.src_usgods`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.src_hpbi`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.src_gis`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.src_fwds`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.ent_cust_ord_actvy`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.src_iex`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust_actvy`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.src_rwms`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.ent_usage_rated_tv`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL


  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.src_invisawear`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.ent_cust_intractn_ivr`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.src_tcom`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.ent_party_identity_cntct`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.src_lynx`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL


  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.ent_sls_chnl`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.ent_cust_measure_tcm`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.src_odscd`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.src_inssider`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.ent_cust_intractn`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.ent_srvc_performance_wls_probe`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.src_dsm`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.src_genesys`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.src_contentful`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.src_srm`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.src_hsm`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.src_netcracker`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.src_crdb`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.src_formbuilder`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.ent_cust_cust`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.src_sap`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.src_adc`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.ent_srvc_config`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.ent_workforce_team_mbr`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.src_sas`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.src_smarthub`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.src_tmf680`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.src_wls_agent_memo`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.src_wifi`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.src_billods`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.src_tmods`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.ent_common_location`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.int_chat`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.ent_resrc_config`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.ent_party_identity`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL


  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.ent_usage_unrated_ott`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.ent_workforce_wrk_ord`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL


  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.src_kb`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.src_gpone`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 
  UNION ALL

  SELECT 
  * 
  FROM `cio-datahub-enterprise-pr-183a.ent_workforce`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS 

)

SELECT *
FROM table_name_columns
--WHERE UPPER(column_name) LIKE '%SERVICE%CD%'
