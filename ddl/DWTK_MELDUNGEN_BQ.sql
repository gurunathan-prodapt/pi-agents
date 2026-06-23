-- Legacy source: d_ausd_bp_ta_iccid_einzeln.sql (invoked by k_ausd_bp_ta_iccid_einzeln.ksh)
-- BigQuery migration for job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_iccid_einzeln.ksh
-- Placeholder DDL for DWTK_MELDUNGEN table.
-- Actual schema should be derived from the source Oracle database.

CREATE TABLE IF NOT EXISTS `your_gcp_project_id.your_bigquery_dataset.DWTK_MELDUNGEN_BQ` (
    `timecreated` TIMESTAMP,
    `job_kennung` STRING,
    -- Add other columns from DWTK_MELDUNGEN as needed
    `payload` JSON
);