-- Query to get customers who switched or upgraded to an Apple Device after July 2021

WITH cleaned_tac_id AS (
SELECT IMSI,MSISDN, TAC_ID, PREV_MONTH_TAC_ID
FROM `cto-wln-sa-data-pr-bb5283.ref_table.bq_exttable_hpbi_mthly_prod_instnc_snapshot_device` 
WHERE 
    LENGTH(TAC_ID)=8 AND 
    LENGTH(PREV_MONTH_TAC_ID) = 8
ORDER BY TAC_ID
), 

tac_info as (
SELECT tac_id, days_since_launch, manufacturer_nm, market_typ, type_txt
FROM (
        SELECT
        cast(t.tac_id as integer) as tac_id,
        DATE_DIFF(CURRENT_DATE(), proj_launch_dt1, DAY) as days_since_launch,

        t.type_txt,
        t.market_typ,
       
        t.manufacturer_nm,
        ROW_NUMBER()
        OVER (
            PARTITION BY t.tac_id
            ORDER BY t.proj_launch_dt1 DESC -- just a heuristic to remove duplicates
             ) as row_num
        FROM `cto-wln-sa-data-pr-bb5283.ref_table.bq_exttable_hive_tac_device_type` t
        -- where t.proj_launch_dt1 IS NOT NULL 
        ) sub
        
WHERE row_num = 1
),


tac_id_manufactuer AS (
    SELECT 
        A.IMSI,
        A.MSISDN,
        CAST(A.TAC_ID AS INTEGER) AS TAC_ID , 
        CAST(A.PREV_MONTH_TAC_ID AS INTEGER) AS PREV_MONTH_TAC_ID,
        B.manufacturer_nm AS cur_manufacturer,
        C.manufacturer_nm AS prev_manufacturer,
        B.days_since_launch AS cur_device_age,
        B.type_txt AS phone_txt
    FROM cleaned_tac_id A 
    INNER JOIN tac_info B 
    ON CAST(A.TAC_ID AS INTEGER) = B.tac_id 
    INNER JOIN tac_info C 
    ON CAST(A.PREV_MONTH_TAC_ID AS INTEGER) = C.tac_id
)

SELECT 
    A.MSISDN, 
    A.IMSI,
    A.TAC_ID,
    CASE WHEN  --If customer changed devices from Apple to Apple or Other manufactuer to Apple, it will be reflected here
        A.cur_manufacturer = 'Apple' AND A.cur_device_age < 1000 THEN 1 
        ELSE 0 
    END AS new_apple_device,
    A.phone_txt,
    A.cur_device_age
FROM tac_id_manufactuer A 
ORDER BY new_apple_device DESC, cur_device_age DESC 

