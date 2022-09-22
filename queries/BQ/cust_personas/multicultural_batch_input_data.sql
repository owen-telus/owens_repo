DECLARE app_names STRING;
DECLARE app_names_processed_pivot STRING;

SET app_names = ("('adt-apps', 'agriculture', 'app stores', 'axion-apps', 'bell-apps', 'bell-fibe-tv-login', 'bell-login', 'bellmts-apps', 'brinkshome-apps', 'business apps', 'cellphones-apps', 'chatr-login', 'chatrwireless-apps', 'chebucto-apps', 'cikbusiness-apps', 'ciktel-apps', 'cityfone-apps', 'cogeco-apps', 'comparecellular-apps', 'comwave-apps', 'cryptocurrency', 'dcitelecom-apps', 'distributel-apps', 'dmts-apps', 'eastlink-apps', 'ebox-apps', 'email', 'ethnic-china', 'ethnic-india', 'ethnic-pakistan', 'ethnic-philippine', 'ethnic-ukraine-russia', 'execulink-apps', 'fido-apps', 'fido-login', 'file transfer', 'fitness', 'fitness apps', 'fizz-apps', 'fleet', 'food-delivery', 'freedommobile-apps', 'freedommobile-login', 'frontpointsecurity-apps', 'gaming', 'gaming platform', 'goabode-apps', 'good2gomobile-apps', 'honeywellhome-apps', 'icewireless-apps', 'iot-shs', 'iPhone 100 - Double mins', 'iPhone 100 - Unlimited msg', 'iPhone 50-200Local & 500MB', 'iPhone 75-200Nat. & 6GB', 'kids apps', 'knet-apps', 'luckymobile-apps', 'lucky-mobile-login', 'messaging', 'misc apps', 'mnsi-apps', 'mobility', 'ms-teams', 'myaccess-apps', 'ncf-apps', 'network operation', 'news', 'northerntel-apps', 'novusnow-apps', 'nwtel-apps', 'ontera-apps', 'other', 'others', 'pcmobile-apps', 'peer to peer', 'peloton', 'pet apps', 'petro-canada-apps', 'planhub-apps', 'primus-apps', 'qiniq-apps', 'real estate', 'real-estate', 'realstate', 'ring-apps', 'rogers-apps', 'rogers-login', 'sasktel-apps', 'security', 'security apps', 'shawdirect-apps', 'shaw-login', 'shawmobile-apps', 'shawmobile-login', 'shs', 'simplyconnect-apps', 'smarthome', 'smarthome-only-products', 'sogetel-apps', 'sourcecable-apps', 'speakout7eleven-apps', 'sports_baseball', 'sports_basketball', 'sports_football', 'sports_generic', 'sports_hockey', 'sports_soccer', 'strava', 'streaming applications', 'streaming-applications', 'tbaytel-apps', 'teksavvy-apps', 'telebec-apps', 'telus apps', 'test applications', 'themobileshop-apps', 'themonitoringcenter-apps', 'thinkprotection-apps', 'tnwcorp-apps', 'tos', 'travel', 'uncategorised ip traffic', 'university-admissions', 'university-ranks', 'updates', 'velcom-apps', 'videotron-apps', 'videotron-login', 'virginmobile-apps', 'virgin-mobile-login', 'virtual care apps', 'vivint-apps', 'vmedia-apps', 'voice over ip', 'web applications', 'whistleout-apps', 'wireless-only-products', 'wireless-plan-comparison', 'wireless-wireline-plan-comparison', 'wireless-wireline-products', 'wireless-wireline-smarthome-products', 'wireline-only-products', 'wireline-smarthome-products', 'wowmobile-apps', 'xploremobile-apps', 'xplornet-apps', 'zoomerwireless-apps')");
--SET app_names = ("('news', 'security', 'misc apps')"); --apps may have spaces

EXECUTE IMMEDIATE format("""
  CREATE TEMP TABLE temp_app_usage AS 
    SELECT
      event_dt,
      imsi_num,
      REGEXP_REPLACE(TRIM(LOWER(app_group_nm)),r'[^a-zA-Z]','') AS app_group_nm,
      SUM(ul_volume_qty/1000000.0 + dl_volume_qty/1000000.0) as total_usage_mb,
      -- 'true' as app_flag
    FROM
      `cio-datahub-enterprise-pr-183a.ent_usage_unrated_ott.bq_ott_app_event`
    WHERE 
      DATE(event_dt) >= DATE_SUB(DATE "2022-06-07", INTERVAL 7 DAY) -- change date format in python date filter is DATE(event_dt) > Current day - X days
      AND imsi_num = '302220320550843'
      AND app_group_nm in %s
    GROUP BY 
      imsi_num, 
      event_dt,
      REGEXP_REPLACE(TRIM(LOWER(app_group_nm)),r'[^a-zA-Z]','')  """, app_names);


SET app_names_processed_pivot = (SELECT CONCAT('("', STRING_AGG(DISTINCT app_group_nm, '", "'), '")') FROM temp_app_usage);


EXECUTE IMMEDIATE format("""
  WITH hpbi_prod_instnc AS (
    SELECT PROD_INSTNC_RESRC_STR as MSISDN,
    PROD_INSTNC_ALIAS_STR as IMSI,
    CUST_TYPE_CD,
    BRAND_TXT,
    BACCT_BILLING_CYCLE_TXT,
    DEVICE_MANUFACTURER_TXT,
    BACCT_BILLING_CYCLE_CD,
    CUST_FIRST_NM,
    CUST_LAST_NM,
    MUNICIPALITY_NM,
    POSTAL_CD,
    PROVINCE_STATE_CD,
    BILLING_ACCOUNT_NUMBER,
    DEACTIVATION_DT,
    PROD_INSTNC_ACTVY_TYPE_CD
    FROM
        `cto-wln-sa-data-pr-bb5283.ref_table.bq_hpbi_product_instance_profl`
    WHERE
        PROD_INSTNC_ALIAS_STR is not null
        and PROD_INSTNC_MASTER_SRC_ID = 130
        AND deactivation_dt is null
        AND BRAND_TXT = 'Telus regular brand'
        AND CUST_TYPE_CD = 'R'
  ),
  lookup_tbl AS (
      SELECT
          string_field_0 as Last_nm,
          string_field_1 as Segment
      FROM
      `cto-wln-sa-data-pr-bb5283.customer_personas_multicultural_model.Multicultural Lookup Tables`
  ),

  app_usage AS (
    
    SELECT
      *
    FROM
      temp_app_usage
    PIVOT (
      AVG(total_usage_mb) AS usage_mb
      FOR app_group_nm IN %s

    )
  )

SELECT
  *
FROM hpbi_prod_instnc

LEFT JOIN lookup_tbl ON hpbi_prod_instnc.CUST_LAST_NM = lookup_tbl.Last_nm -- Change the LEFT JOIN for Batch Prediction Dataset
LEFT JOIN app_usage ON CAST(app_usage.imsi_num as string) = CAST(hpbi_prod_instnc.IMSI as string) -- Change the LEFT JOIN for Batch Prediction Dataset

 """, app_names_processed_pivot);


