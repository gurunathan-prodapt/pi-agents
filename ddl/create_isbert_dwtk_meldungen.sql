-- BigQuery DDL for table: isbert_dwtk_meldungen
-- Legacy Source: isbert_schema.dwtk_meldungen (Oracle)
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_apn_vertrag.ksh

CREATE TABLE IF NOT EXISTS `your_gcp_project.your_bigquery_dataset.isbert_dwtk_meldungen`
(
    job_kennung STRING NOT NULL OPTIONS(description="Job Identifier"),
    timecreated TIMESTAMP NOT NULL OPTIONS(description="Timestamp of creation")
)
PARTITION BY DATE(timecreated);