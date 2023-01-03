-- SP to append weekly data usage to `cto-wln-sa-data-pr-bb5283.customer_personas_features.imsi_app_usage_trends_weekly_ts`

CREATE OR REPLACE PROCEDURE `cto-wln-sa-data-pr-bb5283.customer_personas_features.imsi_app_usage_trends_weekly_ts_append`()
BEGIN 

-- Get past week's data usage aggregated 

EXECUTE IMMEDIATE """
  CREATE TEMP TABLE weekly_data_agg AS (
    WITH time_index AS (
      SELECT
        dt,
        DATE_TRUNC(dt, WEEK(MONDAY)) AS start_dt, -- This gets the Monday of that week (start_date)
        
        -- Only when dt is not on a Sunday, add a week
        CASE WHEN EXTRACT(DAYOFWEEK FROM dt) != 1 
          THEN DATE_ADD(DATE_TRUNC(dt, WEEK(SUNDAY)), INTERVAL 1 WEEK)  -- 1 = Sunday, 7 = Saturday
          ELSE DATE_TRUNC(dt, WEEK(SUNDAY))
        END AS end_dt, 

      -- Regardless of Start Date, query is now robust to have the same start and end date on Monday -> Sunday regardless of when query is ran
      -- Change first date to min(event_dt) from ott
      FROM UNNEST(GENERATE_DATE_ARRAY((SELECT MAX(start_dt) FROM `cto-wln-sa-data-pr-bb5283.customer_personas_features.imsi_app_usage_trends_weekly_ts`), CURRENT_DATE(), INTERVAL 1 WEEK)) as dt 

    ),

    time_index_append AS (
      SELECT
        (SELECT MAX(ts_index) FROM `cto-wln-sa-data-pr-bb5283.customer_personas_features.imsi_app_usage_trends_weekly_ts`) + ROW_NUMBER() OVER(ORDER BY dt ASC) as ts_index,
        * EXCEPT(dt), 
      FROM time_index
      WHERE 
        -- First Monday Date has to be after latest start date in table
        start_dt > (SELECT MAX(start_dt) FROM `cto-wln-sa-data-pr-bb5283.customer_personas_features.imsi_app_usage_trends_weekly_ts`) 
        AND
        -- Last Sunday has to be before last date in app usage, as GENERATE_DATE_ARRAY() won't generate dates past CURRENT_DATE()
        -- This below will be my filter
        end_dt <= (SELECT MAX(event_dt) FROM `cio-datahub-enterprise-pr-183a.ent_usage_unrated_ott.bq_ott_app_event`) 
    ),

    distinct_imsi AS (
        SELECT DISTINCT imsi_num 
        FROM `cio-datahub-enterprise-pr-183a.ent_usage_unrated_ott.bq_ott_app_event`
      ),

    ts_index_imsi AS (
      SELECT 
        A.*,
        B.imsi_num
      FROM time_index_append A
      CROSS JOIN distinct_imsi B

    ),

    app_usage AS (
      SELECT
        A.event_dt,
        A.imsi_num,
        CONCAT(REGEXP_REPLACE(TRIM(LOWER(B.tier_1)),r'[^a-zA-Z]',''), '_', REGEXP_REPLACE(TRIM(LOWER(B.tier_2)),r'[^a-zA-Z]','')) AS category,
        ROUND(SUM(SAFE_DIVIDE(A.ul_volume_qty + A.dl_volume_qty, 1000000.0)), 3) AS usage_volume_MB
      FROM `cio-datahub-enterprise-pr-183a.ent_usage_unrated_ott.bq_ott_app_event` A
      LEFT JOIN `cto-wln-sa-data-pr-bb5283.app_cat_map.bq_app_cat_mapping_latest_view` B 
      ON A.app_nm = B.app_nm
      GROUP BY A.imsi_num, A.event_dt, CONCAT(REGEXP_REPLACE(TRIM(LOWER(B.tier_1)),r'[^a-zA-Z]',''), '_', REGEXP_REPLACE(TRIM(LOWER(B.tier_2)),r'[^a-zA-Z]',''))
    ),

    app_usage_pivot AS (
      SELECT
        *
      FROM app_usage
      PIVOT(
          MAX(usage_volume_MB) AS usage_volume_MB
          FOR category in ("communication_talking", "travel_trips", "telecom_wirelesswirelinesmarthome", "gaming_kids", "productivity_webbrowser", "finance_investing", "news_politics", "lifestyle_mentalhealth", "news_finance", "network_protocol", "travel_airport", "telecom_pricecomparison", "telecom_smarthome", "shopping_food", "finance_utilities", "pets_cats", "lifestyle_healthcare", "network_security", "gaming_moderate", "smarthome_security", "network_other", "entertainment_radio", "entertainment_sports", "entertainment_live", "productivity_softwaredevelopment", "agriculture_farmmanagement", "telecom_wirelesswireline", "travel_fleetmanagement", "travel_navigation", "pets_other", "productivity_education", "smarthome_automation", "realestate_landlord", "entertainment_reading", "os_appstore", "entertainment_news", "shopping_miscellaneous", "travel_lodging", "productivity_filetransfer", "finance_banking", "entertainment_tvandmovies", "travel_ridehailing", "travel_flights", "telecom_telus", "communication_messaging", "realestate_moving", "productivity_other", "pets_dogs", "entertainment_kids", "entertainment_music", "productivity_email", "utility_security", "utility_devicecare", "lifestyle_kids", "news_generic", "realestate_buysellrent", "lifestyle_relationships", "shopping_fashion", "travel_smartcar", "pets_petssupplies", "productivity_editingsoftware", "telecom_others", "telecom_benchmark", "productivity_storage", "news_weather", "travel_travel", "finance_business", "finance_moneytransfer", "lifestyle_fitness", "communication_calling", "lifestyle_identification", "telecom_wireline", "telecom_wireless", "agriculture_farmassociations", "os_update", "productivity_professional", "os_other", "gaming_cloud", "gaming_casual", "communication_miscellaneous", "communication_socialmedia", "entertainment_miscellaneous", "realestate_projectmanagement", "finance_paymentapps")
      )
    ),

    app_usage_weekly_stage AS (
      SELECT
        A.*,
        B.* EXCEPT(imsi_num)
      FROM ts_index_imsi A 
      LEFT JOIN app_usage_pivot B 
      ON
        B.event_dt >= A.start_dt AND B.event_dt <= A.end_dt
        AND A.imsi_num = B.imsi_num
    ),

    app_usage_weekly AS (
      SELECT 
        imsi_num,
        ts_index,
        start_dt,
        end_dt,
        IFNULL(ROUND(SUM(usage_volume_MB_entertainment_sports),2), 0) AS usage_volume_MB_entertainment_sports, 
        IFNULL(ROUND(SUM(usage_volume_MB_realestate_landlord),2), 0) AS usage_volume_MB_realestate_landlord, 
        IFNULL(ROUND(SUM(usage_volume_MB_agriculture_farmmanagement),2), 0) AS usage_volume_MB_agriculture_farmmanagement, 
        IFNULL(ROUND(SUM(usage_volume_MB_productivity_education),2), 0) AS usage_volume_MB_productivity_education, 
        IFNULL(ROUND(SUM(usage_volume_MB_telecom_telus),2), 0) AS usage_volume_MB_telecom_telus, 
        IFNULL(ROUND(SUM(usage_volume_MB_realestate_buysellrent),2), 0) AS usage_volume_MB_realestate_buysellrent, 
        IFNULL(ROUND(SUM(usage_volume_MB_travel_trips),2), 0) AS usage_volume_MB_travel_trips, 
        IFNULL(ROUND(SUM(usage_volume_MB_finance_banking),2), 0) AS usage_volume_MB_finance_banking, 
        IFNULL(ROUND(SUM(usage_volume_MB_pets_dogs),2), 0) AS usage_volume_MB_pets_dogs, 
        IFNULL(ROUND(SUM(usage_volume_MB_lifestyle_mentalhealth),2), 0) AS usage_volume_MB_lifestyle_mentalhealth, 
        IFNULL(ROUND(SUM(usage_volume_MB_os_appstore),2), 0) AS usage_volume_MB_os_appstore, 
        IFNULL(ROUND(SUM(usage_volume_MB_news_generic),2), 0) AS usage_volume_MB_news_generic, 
        IFNULL(ROUND(SUM(usage_volume_MB_entertainment_music),2), 0) AS usage_volume_MB_entertainment_music, 
        IFNULL(ROUND(SUM(usage_volume_MB_communication_miscellaneous),2), 0) AS usage_volume_MB_communication_miscellaneous, 
        IFNULL(ROUND(SUM(usage_volume_MB_gaming_moderate),2), 0) AS usage_volume_MB_gaming_moderate, 
        IFNULL(ROUND(SUM(usage_volume_MB_utility_security),2), 0) AS usage_volume_MB_utility_security, 
        IFNULL(ROUND(SUM(usage_volume_MB_communication_socialmedia),2), 0) AS usage_volume_MB_communication_socialmedia, 
        IFNULL(ROUND(SUM(usage_volume_MB_productivity_other),2), 0) AS usage_volume_MB_productivity_other, 
        IFNULL(ROUND(SUM(usage_volume_MB_shopping_fashion),2), 0) AS usage_volume_MB_shopping_fashion, 
        IFNULL(ROUND(SUM(usage_volume_MB_productivity_webbrowser),2), 0) AS usage_volume_MB_productivity_webbrowser, 
        IFNULL(ROUND(SUM(usage_volume_MB_finance_utilities),2), 0) AS usage_volume_MB_finance_utilities, 
        IFNULL(ROUND(SUM(usage_volume_MB_travel_travel),2), 0) AS usage_volume_MB_travel_travel, 
        IFNULL(ROUND(SUM(usage_volume_MB_lifestyle_fitness),2), 0) AS usage_volume_MB_lifestyle_fitness, 
        IFNULL(ROUND(SUM(usage_volume_MB_finance_business),2), 0) AS usage_volume_MB_finance_business, 
        IFNULL(ROUND(SUM(usage_volume_MB_lifestyle_relationships),2), 0) AS usage_volume_MB_lifestyle_relationships, 
        IFNULL(ROUND(SUM(usage_volume_MB_telecom_wireline),2), 0) AS usage_volume_MB_telecom_wireline, 
        IFNULL(ROUND(SUM(usage_volume_MB_travel_lodging),2), 0) AS usage_volume_MB_travel_lodging, 
        IFNULL(ROUND(SUM(usage_volume_MB_productivity_editingsoftware),2), 0) AS usage_volume_MB_productivity_editingsoftware, 
        IFNULL(ROUND(SUM(usage_volume_MB_pets_petssupplies),2), 0) AS usage_volume_MB_pets_petssupplies, 
        IFNULL(ROUND(SUM(usage_volume_MB_travel_ridehailing),2), 0) AS usage_volume_MB_travel_ridehailing, 
        IFNULL(ROUND(SUM(usage_volume_MB_agriculture_farmassociations),2), 0) AS usage_volume_MB_agriculture_farmassociations, 
        IFNULL(ROUND(SUM(usage_volume_MB_productivity_professional),2), 0) AS usage_volume_MB_productivity_professional, 
        IFNULL(ROUND(SUM(usage_volume_MB_smarthome_automation),2), 0) AS usage_volume_MB_smarthome_automation, 
        IFNULL(ROUND(SUM(usage_volume_MB_lifestyle_healthcare),2), 0) AS usage_volume_MB_lifestyle_healthcare, 
        IFNULL(ROUND(SUM(usage_volume_MB_entertainment_tvandmovies),2), 0) AS usage_volume_MB_entertainment_tvandmovies, 
        IFNULL(ROUND(SUM(usage_volume_MB_travel_flights),2), 0) AS usage_volume_MB_travel_flights, 
        IFNULL(ROUND(SUM(usage_volume_MB_gaming_casual),2), 0) AS usage_volume_MB_gaming_casual, 
        IFNULL(ROUND(SUM(usage_volume_MB_finance_paymentapps),2), 0) AS usage_volume_MB_finance_paymentapps, 
        IFNULL(ROUND(SUM(usage_volume_MB_gaming_kids),2), 0) AS usage_volume_MB_gaming_kids, 
        IFNULL(ROUND(SUM(usage_volume_MB_travel_navigation),2), 0) AS usage_volume_MB_travel_navigation, 
        IFNULL(ROUND(SUM(usage_volume_MB_utility_devicecare),2), 0) AS usage_volume_MB_utility_devicecare, 
        IFNULL(ROUND(SUM(usage_volume_MB_telecom_wirelesswireline),2), 0) AS usage_volume_MB_telecom_wirelesswireline, 
        IFNULL(ROUND(SUM(usage_volume_MB_entertainment_news),2), 0) AS usage_volume_MB_entertainment_news, 
        IFNULL(ROUND(SUM(usage_volume_MB_telecom_others),2), 0) AS usage_volume_MB_telecom_others, 
        IFNULL(ROUND(SUM(usage_volume_MB_news_weather),2), 0) AS usage_volume_MB_news_weather, 
        IFNULL(ROUND(SUM(usage_volume_MB_entertainment_radio),2), 0) AS usage_volume_MB_entertainment_radio, 
        IFNULL(ROUND(SUM(usage_volume_MB_travel_airport),2), 0) AS usage_volume_MB_travel_airport, 
        IFNULL(ROUND(SUM(usage_volume_MB_entertainment_live),2), 0) AS usage_volume_MB_entertainment_live, 
        IFNULL(ROUND(SUM(usage_volume_MB_telecom_pricecomparison),2), 0) AS usage_volume_MB_telecom_pricecomparison, 
        IFNULL(ROUND(SUM(usage_volume_MB_pets_other),2), 0) AS usage_volume_MB_pets_other, 
        IFNULL(ROUND(SUM(usage_volume_MB_lifestyle_identification),2), 0) AS usage_volume_MB_lifestyle_identification, 
        IFNULL(ROUND(SUM(usage_volume_MB_realestate_projectmanagement),2), 0) AS usage_volume_MB_realestate_projectmanagement, 
        IFNULL(ROUND(SUM(usage_volume_MB_communication_talking),2), 0) AS usage_volume_MB_communication_talking, 
        IFNULL(ROUND(SUM(usage_volume_MB_network_security),2), 0) AS usage_volume_MB_network_security, 
        IFNULL(ROUND(SUM(usage_volume_MB_realestate_moving),2), 0) AS usage_volume_MB_realestate_moving, 
        IFNULL(ROUND(SUM(usage_volume_MB_productivity_email),2), 0) AS usage_volume_MB_productivity_email, 
        IFNULL(ROUND(SUM(usage_volume_MB_telecom_wireless),2), 0) AS usage_volume_MB_telecom_wireless, 
        IFNULL(ROUND(SUM(usage_volume_MB_travel_fleetmanagement),2), 0) AS usage_volume_MB_travel_fleetmanagement, 
        IFNULL(ROUND(SUM(usage_volume_MB_communication_messaging),2), 0) AS usage_volume_MB_communication_messaging, 
        IFNULL(ROUND(SUM(usage_volume_MB_gaming_cloud),2), 0) AS usage_volume_MB_gaming_cloud, 
        IFNULL(ROUND(SUM(usage_volume_MB_os_other),2), 0) AS usage_volume_MB_os_other, 
        IFNULL(ROUND(SUM(usage_volume_MB_entertainment_reading),2), 0) AS usage_volume_MB_entertainment_reading, 
        IFNULL(ROUND(SUM(usage_volume_MB_finance_moneytransfer),2), 0) AS usage_volume_MB_finance_moneytransfer, 
        IFNULL(ROUND(SUM(usage_volume_MB_telecom_wirelesswirelinesmarthome),2), 0) AS usage_volume_MB_telecom_wirelesswirelinesmarthome, 
        IFNULL(ROUND(SUM(usage_volume_MB_news_politics),2), 0) AS usage_volume_MB_news_politics, 
        IFNULL(ROUND(SUM(usage_volume_MB_telecom_smarthome),2), 0) AS usage_volume_MB_telecom_smarthome, 
        IFNULL(ROUND(SUM(usage_volume_MB_productivity_softwaredevelopment),2), 0) AS usage_volume_MB_productivity_softwaredevelopment, 
        IFNULL(ROUND(SUM(usage_volume_MB_lifestyle_kids),2), 0) AS usage_volume_MB_lifestyle_kids, 
        IFNULL(ROUND(SUM(usage_volume_MB_telecom_benchmark),2), 0) AS usage_volume_MB_telecom_benchmark, 
        IFNULL(ROUND(SUM(usage_volume_MB_os_update),2), 0) AS usage_volume_MB_os_update, 
        IFNULL(ROUND(SUM(usage_volume_MB_communication_calling),2), 0) AS usage_volume_MB_communication_calling, 
        IFNULL(ROUND(SUM(usage_volume_MB_entertainment_miscellaneous),2), 0) AS usage_volume_MB_entertainment_miscellaneous, 
        IFNULL(ROUND(SUM(usage_volume_MB_network_protocol),2), 0) AS usage_volume_MB_network_protocol, 
        IFNULL(ROUND(SUM(usage_volume_MB_smarthome_security),2), 0) AS usage_volume_MB_smarthome_security, 
        IFNULL(ROUND(SUM(usage_volume_MB_pets_cats),2), 0) AS usage_volume_MB_pets_cats, 
        IFNULL(ROUND(SUM(usage_volume_MB_news_finance),2), 0) AS usage_volume_MB_news_finance, 
        IFNULL(ROUND(SUM(usage_volume_MB_productivity_filetransfer),2), 0) AS usage_volume_MB_productivity_filetransfer, 
        IFNULL(ROUND(SUM(usage_volume_MB_entertainment_kids),2), 0) AS usage_volume_MB_entertainment_kids, 
        IFNULL(ROUND(SUM(usage_volume_MB_finance_investing),2), 0) AS usage_volume_MB_finance_investing, 
        IFNULL(ROUND(SUM(usage_volume_MB_shopping_food),2), 0) AS usage_volume_MB_shopping_food, 
        IFNULL(ROUND(SUM(usage_volume_MB_shopping_miscellaneous),2), 0) AS usage_volume_MB_shopping_miscellaneous, 
        IFNULL(ROUND(SUM(usage_volume_MB_network_other),2), 0) AS usage_volume_MB_network_other, 
        IFNULL(ROUND(SUM(usage_volume_MB_productivity_storage),2), 0) AS usage_volume_MB_productivity_storage, 
        IFNULL(ROUND(SUM(usage_volume_MB_travel_smartcar),2), 0) AS usage_volume_MB_travel_smartcar


      FROM app_usage_weekly_stage
      GROUP BY imsi_num, ts_index, start_dt, end_dt
      
    )

    SELECT * 
    FROM app_usage_weekly 

  )
""";

-- Merge into Existing Table
EXECUTE IMMEDIATE """
  MERGE INTO `cto-wln-sa-data-pr-bb5283.customer_personas_features.imsi_app_usage_trends_weekly_ts` AS target_table
  USING weekly_data_agg AS source_table 
  ON
    target_table.imsi_num = source_table.imsi_num AND 
    target_table.ts_index = source_table.ts_index AND 
    target_table.start_dt = source_table.start_dt AND 
    target_table.end_dt = source_table.end_dt 

  WHEN NOT MATCHED THEN 
    INSERT (
      imsi_num,
      ts_index,
      start_dt,
      end_dt,
      usage_volume_MB_entertainment_sports,
      usage_volume_MB_realestate_landlord,
      usage_volume_MB_agriculture_farmmanagement,
      usage_volume_MB_productivity_education,
      usage_volume_MB_telecom_telus,
      usage_volume_MB_realestate_buysellrent,
      usage_volume_MB_travel_trips,
      usage_volume_MB_finance_banking,
      usage_volume_MB_pets_dogs,
      usage_volume_MB_lifestyle_mentalhealth,
      usage_volume_MB_os_appstore,
      usage_volume_MB_news_generic,
      usage_volume_MB_entertainment_music,
      usage_volume_MB_communication_miscellaneous,
      usage_volume_MB_gaming_moderate,
      usage_volume_MB_utility_security,
      usage_volume_MB_communication_socialmedia,
      usage_volume_MB_productivity_other,
      usage_volume_MB_shopping_fashion,
      usage_volume_MB_productivity_webbrowser,
      usage_volume_MB_finance_utilities,
      usage_volume_MB_travel_travel,
      usage_volume_MB_lifestyle_fitness,
      usage_volume_MB_finance_business,
      usage_volume_MB_lifestyle_relationships,
      usage_volume_MB_telecom_wireline,
      usage_volume_MB_travel_lodging,
      usage_volume_MB_productivity_editingsoftware,
      usage_volume_MB_pets_petssupplies,
      usage_volume_MB_travel_ridehailing,
      usage_volume_MB_agriculture_farmassociations,
      usage_volume_MB_productivity_professional,
      usage_volume_MB_smarthome_automation,
      usage_volume_MB_lifestyle_healthcare,
      usage_volume_MB_entertainment_tvandmovies,
      usage_volume_MB_travel_flights,
      usage_volume_MB_gaming_casual,
      usage_volume_MB_finance_paymentapps,
      usage_volume_MB_gaming_kids,
      usage_volume_MB_travel_navigation,
      usage_volume_MB_utility_devicecare,
      usage_volume_MB_telecom_wirelesswireline,
      usage_volume_MB_entertainment_news,
      usage_volume_MB_telecom_others,
      usage_volume_MB_news_weather,
      usage_volume_MB_entertainment_radio,
      usage_volume_MB_travel_airport,
      usage_volume_MB_entertainment_live,
      usage_volume_MB_telecom_pricecomparison,
      usage_volume_MB_pets_other,
      usage_volume_MB_lifestyle_identification,
      usage_volume_MB_realestate_projectmanagement,
      usage_volume_MB_communication_talking,
      usage_volume_MB_network_security,
      usage_volume_MB_realestate_moving,
      usage_volume_MB_productivity_email,
      usage_volume_MB_telecom_wireless,
      usage_volume_MB_travel_fleetmanagement,
      usage_volume_MB_communication_messaging,
      usage_volume_MB_gaming_cloud,
      usage_volume_MB_os_other,
      usage_volume_MB_entertainment_reading,
      usage_volume_MB_finance_moneytransfer,
      usage_volume_MB_telecom_wirelesswirelinesmarthome,
      usage_volume_MB_news_politics,
      usage_volume_MB_telecom_smarthome,
      usage_volume_MB_productivity_softwaredevelopment,
      usage_volume_MB_lifestyle_kids,
      usage_volume_MB_telecom_benchmark,
      usage_volume_MB_os_update,
      usage_volume_MB_communication_calling,
      usage_volume_MB_entertainment_miscellaneous,
      usage_volume_MB_network_protocol,
      usage_volume_MB_smarthome_security,
      usage_volume_MB_pets_cats,
      usage_volume_MB_news_finance,
      usage_volume_MB_productivity_filetransfer,
      usage_volume_MB_entertainment_kids,
      usage_volume_MB_finance_investing,
      usage_volume_MB_shopping_food,
      usage_volume_MB_shopping_miscellaneous,
      usage_volume_MB_network_other,
      usage_volume_MB_productivity_storage,
      usage_volume_MB_travel_smartcar

    )
    VALUES(
      source_table.imsi_num,
      source_table.ts_index,
      source_table.start_dt,
      source_table.end_dt,
      source_table.usage_volume_MB_entertainment_sports,
      source_table.usage_volume_MB_realestate_landlord,
      source_table.usage_volume_MB_agriculture_farmmanagement,
      source_table.usage_volume_MB_productivity_education,
      source_table.usage_volume_MB_telecom_telus,
      source_table.usage_volume_MB_realestate_buysellrent,
      source_table.usage_volume_MB_travel_trips,
      source_table.usage_volume_MB_finance_banking,
      source_table.usage_volume_MB_pets_dogs,
      source_table.usage_volume_MB_lifestyle_mentalhealth,
      source_table.usage_volume_MB_os_appstore,
      source_table.usage_volume_MB_news_generic,
      source_table.usage_volume_MB_entertainment_music,
      source_table.usage_volume_MB_communication_miscellaneous,
      source_table.usage_volume_MB_gaming_moderate,
      source_table.usage_volume_MB_utility_security,
      source_table.usage_volume_MB_communication_socialmedia,
      source_table.usage_volume_MB_productivity_other,
      source_table.usage_volume_MB_shopping_fashion,
      source_table.usage_volume_MB_productivity_webbrowser,
      source_table.usage_volume_MB_finance_utilities,
      source_table.usage_volume_MB_travel_travel,
      source_table.usage_volume_MB_lifestyle_fitness,
      source_table.usage_volume_MB_finance_business,
      source_table.usage_volume_MB_lifestyle_relationships,
      source_table.usage_volume_MB_telecom_wireline,
      source_table.usage_volume_MB_travel_lodging,
      source_table.usage_volume_MB_productivity_editingsoftware,
      source_table.usage_volume_MB_pets_petssupplies,
      source_table.usage_volume_MB_travel_ridehailing,
      source_table.usage_volume_MB_agriculture_farmassociations,
      source_table.usage_volume_MB_productivity_professional,
      source_table.usage_volume_MB_smarthome_automation,
      source_table.usage_volume_MB_lifestyle_healthcare,
      source_table.usage_volume_MB_entertainment_tvandmovies,
      source_table.usage_volume_MB_travel_flights,
      source_table.usage_volume_MB_gaming_casual,
      source_table.usage_volume_MB_finance_paymentapps,
      source_table.usage_volume_MB_gaming_kids,
      source_table.usage_volume_MB_travel_navigation,
      source_table.usage_volume_MB_utility_devicecare,
      source_table.usage_volume_MB_telecom_wirelesswireline,
      source_table.usage_volume_MB_entertainment_news,
      source_table.usage_volume_MB_telecom_others,
      source_table.usage_volume_MB_news_weather,
      source_table.usage_volume_MB_entertainment_radio,
      source_table.usage_volume_MB_travel_airport,
      source_table.usage_volume_MB_entertainment_live,
      source_table.usage_volume_MB_telecom_pricecomparison,
      source_table.usage_volume_MB_pets_other,
      source_table.usage_volume_MB_lifestyle_identification,
      source_table.usage_volume_MB_realestate_projectmanagement,
      source_table.usage_volume_MB_communication_talking,
      source_table.usage_volume_MB_network_security,
      source_table.usage_volume_MB_realestate_moving,
      source_table.usage_volume_MB_productivity_email,
      source_table.usage_volume_MB_telecom_wireless,
      source_table.usage_volume_MB_travel_fleetmanagement,
      source_table.usage_volume_MB_communication_messaging,
      source_table.usage_volume_MB_gaming_cloud,
      source_table.usage_volume_MB_os_other,
      source_table.usage_volume_MB_entertainment_reading,
      source_table.usage_volume_MB_finance_moneytransfer,
      source_table.usage_volume_MB_telecom_wirelesswirelinesmarthome,
      source_table.usage_volume_MB_news_politics,
      source_table.usage_volume_MB_telecom_smarthome,
      source_table.usage_volume_MB_productivity_softwaredevelopment,
      source_table.usage_volume_MB_lifestyle_kids,
      source_table.usage_volume_MB_telecom_benchmark,
      source_table.usage_volume_MB_os_update,
      source_table.usage_volume_MB_communication_calling,
      source_table.usage_volume_MB_entertainment_miscellaneous,
      source_table.usage_volume_MB_network_protocol,
      source_table.usage_volume_MB_smarthome_security,
      source_table.usage_volume_MB_pets_cats,
      source_table.usage_volume_MB_news_finance,
      source_table.usage_volume_MB_productivity_filetransfer,
      source_table.usage_volume_MB_entertainment_kids,
      source_table.usage_volume_MB_finance_investing,
      source_table.usage_volume_MB_shopping_food,
      source_table.usage_volume_MB_shopping_miscellaneous,
      source_table.usage_volume_MB_network_other,
      source_table.usage_volume_MB_productivity_storage,
      source_table.usage_volume_MB_travel_smartcar

  )
""";

-- Create Aggregated Table with total usage over time of all customers
EXECUTE IMMEDIATE """
  CREATE OR REPLACE TABLE `cto-wln-sa-data-pr-bb5283.customer_personas_features.imsi_app_usage_trends_agg_ts` AS (
    SELECT  
    ts_index,
    start_dt,
    end_dt,
    SUM(usage_volume_MB_entertainment_sports)/1000000.0 AS total_entertainment_sports_usage_TB,
    SUM(usage_volume_MB_realestate_landlord)/1000000.0 AS total_realestate_landlord_usage_TB,
    SUM(usage_volume_MB_agriculture_farmmanagement)/1000000.0 AS total_agriculture_farmmanagement_usage_TB,
    SUM(usage_volume_MB_productivity_education)/1000000.0 AS total_productivity_education_usage_TB,
    SUM(usage_volume_MB_telecom_telus)/1000000.0 AS total_telecom_telus_usage_TB,
    SUM(usage_volume_MB_realestate_buysellrent)/1000000.0 AS total_realestate_buysellrent_usage_TB,
    SUM(usage_volume_MB_travel_trips)/1000000.0 AS total_travel_trips_usage_TB,
    SUM(usage_volume_MB_finance_banking)/1000000.0 AS total_finance_banking_usage_TB,
    SUM(usage_volume_MB_pets_dogs)/1000000.0 AS total_pets_dogs_usage_TB,
    SUM(usage_volume_MB_lifestyle_mentalhealth)/1000000.0 AS total_lifestyle_mentalhealth_usage_TB,
    SUM(usage_volume_MB_os_appstore)/1000000.0 AS total_os_appstore_usage_TB,
    SUM(usage_volume_MB_news_generic)/1000000.0 AS total_news_generic_usage_TB,
    SUM(usage_volume_MB_entertainment_music)/1000000.0 AS total_entertainment_music_usage_TB,
    SUM(usage_volume_MB_communication_miscellaneous)/1000000.0 AS total_communication_miscellaneous_usage_TB,
    SUM(usage_volume_MB_gaming_moderate)/1000000.0 AS total_gaming_moderate_usage_TB,
    SUM(usage_volume_MB_utility_security)/1000000.0 AS total_utility_security_usage_TB,
    SUM(usage_volume_MB_communication_socialmedia)/1000000.0 AS total_communication_socialmedia_usage_TB,
    SUM(usage_volume_MB_productivity_other)/1000000.0 AS total_productivity_other_usage_TB,
    SUM(usage_volume_MB_shopping_fashion)/1000000.0 AS total_shopping_fashion_usage_TB,
    SUM(usage_volume_MB_productivity_webbrowser)/1000000.0 AS total_productivity_webbrowser_usage_TB,
    SUM(usage_volume_MB_finance_utilities)/1000000.0 AS total_finance_utilities_usage_TB,
    SUM(usage_volume_MB_travel_travel)/1000000.0 AS total_travel_travel_usage_TB,
    SUM(usage_volume_MB_lifestyle_fitness)/1000000.0 AS total_lifestyle_fitness_usage_TB,
    SUM(usage_volume_MB_finance_business)/1000000.0 AS total_finance_business_usage_TB,
    SUM(usage_volume_MB_lifestyle_relationships)/1000000.0 AS total_lifestyle_relationships_usage_TB,
    SUM(usage_volume_MB_telecom_wireline)/1000000.0 AS total_telecom_wireline_usage_TB,
    SUM(usage_volume_MB_travel_lodging)/1000000.0 AS total_travel_lodging_usage_TB,
    SUM(usage_volume_MB_productivity_editingsoftware)/1000000.0 AS total_productivity_editingsoftware_usage_TB,
    SUM(usage_volume_MB_pets_petssupplies)/1000000.0 AS total_pets_petssupplies_usage_TB,
    SUM(usage_volume_MB_travel_ridehailing)/1000000.0 AS total_travel_ridehailing_usage_TB,
    SUM(usage_volume_MB_agriculture_farmassociations)/1000000.0 AS total_agriculture_farmassociations_usage_TB,
    SUM(usage_volume_MB_productivity_professional)/1000000.0 AS total_productivity_professional_usage_TB,
    SUM(usage_volume_MB_smarthome_automation)/1000000.0 AS total_smarthome_automation_usage_TB,
    SUM(usage_volume_MB_lifestyle_healthcare)/1000000.0 AS total_lifestyle_healthcare_usage_TB,
    SUM(usage_volume_MB_entertainment_tvandmovies)/1000000.0 AS total_entertainment_tvandmovies_usage_TB,
    SUM(usage_volume_MB_travel_flights)/1000000.0 AS total_travel_flights_usage_TB,
    SUM(usage_volume_MB_gaming_casual)/1000000.0 AS total_gaming_casual_usage_TB,
    SUM(usage_volume_MB_finance_paymentapps)/1000000.0 AS total_finance_paymentapps_usage_TB,
    SUM(usage_volume_MB_gaming_kids)/1000000.0 AS total_gaming_kids_usage_TB,
    SUM(usage_volume_MB_travel_navigation)/1000000.0 AS total_travel_navigation_usage_TB,
    SUM(usage_volume_MB_utility_devicecare)/1000000.0 AS total_utility_devicecare_usage_TB,
    SUM(usage_volume_MB_telecom_wirelesswireline)/1000000.0 AS total_telecom_wirelesswireline_usage_TB,
    SUM(usage_volume_MB_entertainment_news)/1000000.0 AS total_entertainment_news_usage_TB,
    SUM(usage_volume_MB_telecom_others)/1000000.0 AS total_telecom_others_usage_TB,
    SUM(usage_volume_MB_news_weather)/1000000.0 AS total_news_weather_usage_TB,
    SUM(usage_volume_MB_entertainment_radio)/1000000.0 AS total_entertainment_radio_usage_TB,
    SUM(usage_volume_MB_travel_airport)/1000000.0 AS total_travel_airport_usage_TB,
    SUM(usage_volume_MB_entertainment_live)/1000000.0 AS total_entertainment_live_usage_TB,
    SUM(usage_volume_MB_telecom_pricecomparison)/1000000.0 AS total_telecom_pricecomparison_usage_TB,
    SUM(usage_volume_MB_pets_other)/1000000.0 AS total_pets_other_usage_TB,
    SUM(usage_volume_MB_lifestyle_identification)/1000000.0 AS total_lifestyle_identification_usage_TB,
    SUM(usage_volume_MB_realestate_projectmanagement)/1000000.0 AS total_realestate_projectmanagement_usage_TB,
    SUM(usage_volume_MB_communication_talking)/1000000.0 AS total_communication_talking_usage_TB,
    SUM(usage_volume_MB_network_security)/1000000.0 AS total_network_security_usage_TB,
    SUM(usage_volume_MB_realestate_moving)/1000000.0 AS total_realestate_moving_usage_TB,
    SUM(usage_volume_MB_productivity_email)/1000000.0 AS total_productivity_email_usage_TB,
    SUM(usage_volume_MB_telecom_wireless)/1000000.0 AS total_telecom_wireless_usage_TB,
    SUM(usage_volume_MB_travel_fleetmanagement)/1000000.0 AS total_travel_fleetmanagement_usage_TB,
    SUM(usage_volume_MB_communication_messaging)/1000000.0 AS total_communication_messaging_usage_TB,
    SUM(usage_volume_MB_gaming_cloud)/1000000.0 AS total_gaming_cloud_usage_TB,
    SUM(usage_volume_MB_os_other)/1000000.0 AS total_os_other_usage_TB,
    SUM(usage_volume_MB_entertainment_reading)/1000000.0 AS total_entertainment_reading_usage_TB,
    SUM(usage_volume_MB_finance_moneytransfer)/1000000.0 AS total_finance_moneytransfer_usage_TB,
    SUM(usage_volume_MB_telecom_wirelesswirelinesmarthome)/1000000.0 AS total_telecom_wirelesswirelinesmarthome_usage_TB,
    SUM(usage_volume_MB_news_politics)/1000000.0 AS total_news_politics_usage_TB,
    SUM(usage_volume_MB_telecom_smarthome)/1000000.0 AS total_telecom_smarthome_usage_TB,
    SUM(usage_volume_MB_productivity_softwaredevelopment)/1000000.0 AS total_productivity_softwaredevelopment_usage_TB,
    SUM(usage_volume_MB_lifestyle_kids)/1000000.0 AS total_lifestyle_kids_usage_TB,
    SUM(usage_volume_MB_telecom_benchmark)/1000000.0 AS total_telecom_benchmark_usage_TB,
    SUM(usage_volume_MB_os_update)/1000000.0 AS total_os_update_usage_TB,
    SUM(usage_volume_MB_communication_calling)/1000000.0 AS total_communication_calling_usage_TB,
    SUM(usage_volume_MB_entertainment_miscellaneous)/1000000.0 AS total_entertainment_miscellaneous_usage_TB,
    SUM(usage_volume_MB_network_protocol)/1000000.0 AS total_network_protocol_usage_TB,
    SUM(usage_volume_MB_smarthome_security)/1000000.0 AS total_smarthome_security_usage_TB,
    SUM(usage_volume_MB_pets_cats)/1000000.0 AS total_pets_cats_usage_TB,
    SUM(usage_volume_MB_news_finance)/1000000.0 AS total_news_finance_usage_TB,
    SUM(usage_volume_MB_productivity_filetransfer)/1000000.0 AS total_productivity_filetransfer_usage_TB,
    SUM(usage_volume_MB_entertainment_kids)/1000000.0 AS total_entertainment_kids_usage_TB,
    SUM(usage_volume_MB_finance_investing)/1000000.0 AS total_finance_investing_usage_TB,
    SUM(usage_volume_MB_shopping_food)/1000000.0 AS total_shopping_food_usage_TB,
    SUM(usage_volume_MB_shopping_miscellaneous)/1000000.0 AS total_shopping_miscellaneous_usage_TB,
    SUM(usage_volume_MB_network_other)/1000000.0 AS total_network_other_usage_TB,
    SUM(usage_volume_MB_productivity_storage)/1000000.0 AS total_productivity_storage_usage_TB,
    SUM(usage_volume_MB_travel_smartcar)/1000000.0 AS total_travel_smartcar_usage_TB,

  FROM `cto-wln-sa-data-pr-bb5283.customer_personas_features.imsi_app_usage_trends_weekly_ts` 
  GROUP BY ts_index,start_dt, end_dt
  ORDER BY ts_index

  )



""";
END