CREATE OR REPLACE PROCEDURE `cto-wln-sa-data-pr-bb5283.stored_procedure.export_to_workbench`()
BEGIN
----------------------------------------------------------------------------------------------------
-- CHANGE LOG 
----------------------------------------------------------------------------------------------------
-- DATE:        NAME:
-- 2022-06-07   Owen Ren
--            
-- DESCRIPTION: 
-- The purpose of this stored procedure is to share external tables to GTLP AI LC Workbench.
----------------------------------------------------------------------------------------------------

TRUNCATE TABLE `gtlp-ailc-cpod-pr-5cdee4.temp_workspace.tac_info`;

INSERT INTO `gtlp-ailc-cpod-pr-5cdee4.temp_workspace.tac_info`
SELECT * FROM `cto-wln-sa-data-pr-bb5283.customer_personas_features.tac_info`;


    
END;