CREATE OR REPLACE PROCEDURE temp_workspace.app_usage_wip(v_app_proj_name STRING, v_app_dataset_name STRING, v_app_table_name STRING, v_app_column_name STRING, v_dl_vol_column_name STRING, v_ul_vol_column_name STRING, v_date_column_name STRING, v_id_app_column_name STRING, v_cust_dataset_name STRING, v_cust_table_name STRING, v_id_column_name STRING,v_app_mapping_dataset STRING, v_app_mapping_table STRING,v_app_tier1_column_name STRING, v_app_tier2_column_name STRING, v_application_filter_type STRING, v_application_wishlist ARRAY <STRING>, v_application_blacklist ARRAY <STRING>, v_app_cat_tier1 ARRAY <STRING>, v_app_cat_tier2 ARRAY <STRING>, v_date_filter_type STRING, v_today_date DATE, v_date_len INT64, v_date_step STRING, v_start_date DATE, v_end_date DATE, v_segment_name STRING, v_output_dataset_name STRING, v_output_table_name STRING)


BEGIN

    ----------------------------------------------------------------------------------------------------
    -- DECLARE VARIABLES
    ----------------------------------------------------------------------------------------------------
    DECLARE v_sql STRING;
    DECLARE v_array_string_wl STRING;
    DECLARE v_array_string_bl STRING;
    DECLARE v_array_string_t1 STRING;
    DECLARE v_array_string_t2 STRING;
    DECLARE v_date_filter_select STRING;
    DECLARE v_app_filter_select STRING;

    ----------------------------------------------------------------------------------------------------
    -- CONVERT ARRAY TO STRING
    --  Notes:
    --    Array to string function is separating array values by ','. The backslash escapes the quote character.
    --    Single quotes are concatenated on the front and end to construct a single-quoted list.
    ----------------------------------------------------------------------------------------------------


    SET v_array_string_wl = '\''|| ARRAY_TO_STRING(v_application_wishlist, '\',\'') || '\'';
    SET v_array_string_bl = '\''|| ARRAY_TO_STRING(v_application_blacklist, '\',\'') || '\'';
    SET v_array_string_t1 = '\''|| ARRAY_TO_STRING(v_app_cat_tier1, '\',\'') || '\'';
    SET v_array_string_t2 = '\''|| ARRAY_TO_STRING(v_app_cat_tier2, '\',\'') || '\'';


    ----------------------------------------------------------------------------------------------------
    -- ASSIGN FILTER STRING BASED ON FILTER TYPE 
    ----------------------------------------------------------------------------------------------------
    IF v_date_filter_type = 'interval' THEN 
        SET v_date_filter_select = 'AND B.'||v_date_column_name||' BETWEEN \''||v_start_date||'\' AND \''||v_end_date||'\' ' ;   
    ELSE 
        SET v_date_filter_select = 'AND B.'||v_date_column_name||' BETWEEN date_sub('|| '\''|| v_today_date || '\'' ||',INTERVAL ' || v_date_len || ' ' || v_date_step || ')'|| ' AND date_sub('|| '\''|| v_today_date || '\'' ||',INTERVAL 1 DAY) '  ;    
    END IF;

 -- CHECK FOR EMPTY ARRAYS BEFORE ASSIGNING
    IF v_application_filter_type = 'by_app_name' THEN 
        SET v_app_filter_select = 'B.'||v_app_column_name||' NOT IN ('||v_array_string_bl||') ' || 'AND B.'||v_app_column_name||' IN ('||v_array_string_wl||') ' ;   
    ELSE 
        IF ARRAY_LENGTH(v_app_cat_tier2) > 0 THEN
            SET v_app_filter_select = ' C.'||v_app_tier1_column_name||' IN ('||v_array_string_t1|| ') '||  ' AND C.'||v_app_tier2_column_name||' IN ('||v_array_string_t2|| ') ' ;
        ELSE 
            SET v_app_filter_select = ' C.'||v_app_tier1_column_name||' IN ('||v_array_string_t1|| ') ' ;
        END IF;
    END IF;

    ----------------------------------------------------------------------------------------------------
    -- CONSTRUCT DYNAMIC SQL
    ----------------------------------------------------------------------------------------------------
    SET v_sql =
        'CREATE OR REPLACE TABLE ' || v_output_dataset_name || '.' || v_output_table_name || ' AS '||
        'SELECT *, ' || 
        'ROUND (PERCENT_RANK() OVER (ORDER BY ' || v_segment_name || '_dl_data_volume  + ' || v_segment_name ||'_ul_data_volume ASC),3) AS ' || v_segment_name||'_data_volume_rank ' ||
        'FROM ( SELECT A.*,' ||
                'CASE' ||
                'WHEN EXTRACT(DAYOFWEEK FROM B.event_dt) IN (1, 7) THEN \'weekend\'  ELSE \'weekday\' END AS day,' ||
        'COUNT(DISTINCT B.' || v_app_column_name || ') AS '||v_segment_name||'_app_count,'||
        'COUNT(DISTINCT B.' || v_date_column_name || ') AS '||v_segment_name||'_days_frequency,'||
        'SUM(B.'||v_dl_vol_column_name||') AS '||v_segment_name||'_dl_data_volume, '||
        'SUM(B.' || v_ul_vol_column_name||') AS ' || v_segment_name||'_ul_data_volume '||
        'FROM `'||v_cust_dataset_name||'.'||v_cust_table_name||'` A ' ||
        'LEFT JOIN `'||v_app_proj_name||'.'||v_app_dataset_name||'.'||v_app_table_name||'` B '||
        'ON A.'||v_id_column_name||' = B.'||v_id_app_column_name ||
        ' LEFT JOIN `' || v_app_mapping_dataset || '.' || v_app_mapping_table || '` C '||
        'ON B.'||v_app_column_name||'= C.'||v_app_column_name||
        ' WHERE ' || v_app_filter_select ||  v_date_filter_select  ||            
        ' GROUP BY 1,2,3,4,5,6,7,8'||
        ' ORDER BY A.'|| v_id_column_name ||' ASC) results' ||
        'PIVOT (' ||
                'SUM(' ||v_segment_name||'_app_count) AS ' ||v_segment_name||'_app_count,'||
                'SUM(' ||v_segment_name||'_days_frequency) AS ' ||v_segment_name||'_days_frequency,'||
                'SUM(' ||v_segment_name||'_dl_data_volume) AS ' ||v_segment_name||'_dl_data_volume,'||
                'SUM(' ||v_segment_name||'_ul_data_volume) AS ' ||v_segment_name||'_ul_data_volume,'||
                'FOR day' ||
                'IN(\'weekend\', \'weekday\'))';  
    ----------------------------------------------------------------------------------------------------
    -- EXECUTE DYNAMIC SQL
    ----------------------------------------------------------------------------------------------------
    EXECUTE IMMEDIATE v_sql;

END
