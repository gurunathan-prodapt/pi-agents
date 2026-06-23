-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_da_vda_tk.ksh
-- DDL for the error logging table.

CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.error_log` (
    job_id STRING NOT NULL,
    error_message STRING NOT NULL,
    error_details STRING,
    created_at TIMESTAMP NOT NULL
)
OPTIONS(
  description="Log table for errors in migrated ETL jobs."
);