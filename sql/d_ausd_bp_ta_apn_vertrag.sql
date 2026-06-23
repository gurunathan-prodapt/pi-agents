-- BigQuery SQL for data transformation
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aufbereitung/sql/d_ausd_bp_ta_apn_vertrag.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_apn_vertrag.ksh

DECLARE v_snapshot_date DATE DEFAULT (
  SELECT COALESCE(MAX(DATE(timecreated)), DATE '1900-01-01')
  FROM `your_gcp_project.your_bigquery_dataset.isbert_dwtk_meldungen`
  WHERE job_kennung = 'BERT_DROP_TEMP_TABLE'
);

DELETE FROM `your_gcp_project.your_bigquery_dataset.sof_ta_apn_vertrag`
WHERE snapshot_date = v_snapshot_date;

INSERT INTO `your_gcp_project.your_bigquery_dataset.sof_ta_apn_vertrag`
  (cntrct_id, apn_list, contract_ref_list, snapshot_date)
SELECT
  cntrct_id,
  SUBSTR(STRING_AGG(access_point_name, ', ' ORDER BY access_point_name), 1, 100) AS apn_list,
  SUBSTR(STRING_AGG(cntrct_id_ref, ', ' ORDER BY cntrct_id_ref), 1, 100) AS contract_ref_list,
  v_snapshot_date AS snapshot_date
FROM `your_gcp_project.your_bigquery_dataset.sof_ta_bpr_apn`
GROUP BY cntrct_id;