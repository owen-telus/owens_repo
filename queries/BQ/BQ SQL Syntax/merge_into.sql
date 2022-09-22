-- Merge into table on date and imsi 

MERGE INTO `cto-wln-sa-data-pr-bb5283.temp_workspace.date_index` T
USING 
  (
    SELECT 
      11 AS ts_index,
      DATE('2022-08-27') AS start_dt,
      DATE('2022-09-05') AS end_dt, 
      'ABC' AS imsi
  ) S

ON 
  T.imsi = S.imsi 
  AND T.start_dt = S.start_dt
  AND T.ts_index = S.ts_index
  AND T.end_dt = S.end_dt

WHEN NOT MATCHED THEN 
  INSERT (
    ts_index,
    start_dt,
    end_dt,
    imsi
  )

  VALUES(
    S.ts_index,
    S.start_dt,
    S.end_dt,
    S.imsi
  )


