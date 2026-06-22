-- BigQuery DDL for the dwtk_meldungen_bq table
-- Replaces: isbert_schema.dwtk_meldungen used by vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_inv_assign.ksh
-- This table is assumed to store logging/metadata information.

CREATE TABLE IF NOT EXISTS `your-gcp-project.isbert_log_data.dwtk_meldungen_bq` (
    timecreated TIMESTAMP,
    job_kennung STRING NOT NULL
);

-- Example: Insert a dummy row for 'BERT_DROP_TEMP_TABLE' if it doesn't exist for testing/initial setup
-- You might want to remove this for production or manage inserts through a separate process.
INSERT INTO `your-gcp-project.isbert_log_data.dwtk_meldungen_bq` (timecreated, job_kennung)
SELECT CURRENT_TIMESTAMP(), 'BERT_DROP_TEMP_TABLE'
WHERE NOT EXISTS (SELECT 1 FROM `your-gcp-project.isbert_log_data.dwtk_meldungen_bq` WHERE job_kennung = 'BERT_DROP_TEMP_TABLE');