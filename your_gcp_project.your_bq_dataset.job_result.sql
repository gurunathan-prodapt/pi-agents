--
-- DDL for BigQuery table: your_gcp_project.your_bq_dataset.job_result
-- Replaces temporary file usage for record counts in vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_bp_ref.ksh
--
CREATE TABLE your_gcp_project.your_bq_dataset.job_result (
    job_kennung STRING NOT NULL OPTIONS(description="Unique identifier for the job"),
    eintrags_nr STRING NOT NULL OPTIONS(description="Entry number for the job instance"),
    record_count INT64 OPTIONS(description="Number of records processed or inserted"),
    created_ts DATETIME NOT NULL OPTIONS(description="Timestamp when the result was recorded")
);