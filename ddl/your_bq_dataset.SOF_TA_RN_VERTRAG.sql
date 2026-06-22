-- BigQuery DDL for SOF$TA_RN_VERTRAG
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_vertrag.ksh
-- This table is populated by the BigQuery Stored Procedure r_ausd_bp_ta_rn_vertrag.

CREATE TABLE IF NOT EXISTS `your_gcp_project.your_bq_dataset.SOF$TA_RN_VERTRAG`
(
  MELDUNG_CD           STRING,
  MELDUNG_TEXT         STRING,
  EINTRAG_NR           STRING,
  VERTRAG_NR           STRING,
  MELDUNG_GUELTIG_CD   STRING,
  MELDUNG_GUELTIG_TEXT STRING,
  -- Adding a processing timestamp for auditing and potential identification of current run's inserts
  PROCESSING_TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);