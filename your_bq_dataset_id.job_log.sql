-- Target for legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_discount.ksh
-- Description: BigQuery table to capture job execution logs.
CREATE TABLE your_gcp_project_id.your_bq_dataset_id.job_log (
    job_entry_nr INT64 NOT NULL,
    job_kennung STRING NOT NULL,
    script_name STRING,
    log_timestamp TIMESTAMP NOT NULL,
    log_level STRING,
    message STRING
);