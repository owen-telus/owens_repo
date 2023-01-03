TRUNCATE TABLE `roaming-pr-66a1b0.roaming_pr.bq_v2_pre_trip_predict_non_ts`;

INSERT INTO `roaming-pr-66a1b0.roaming_pr.bq_v2_pre_trip_predict_non_ts`
SELECT * FROM `bi-stg-aaaie-pr-750ff7.roaming_pr.bq_v2_pre_trip_predict_non_ts`;



TRUNCATE TABLE `roaming-pr-66a1b0.roaming_pr.bq_v2_pre_trip_predict_ts`;

INSERT INTO `roaming-pr-66a1b0.roaming_pr.bq_v2_pre_trip_predict_ts`
SELECT * FROM `bi-stg-aaaie-pr-750ff7.roaming_pr.bq_v2_pre_trip_predict_ts`;

