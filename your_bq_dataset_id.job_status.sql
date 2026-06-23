-- Target for legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_discount.ksh
-- Description: BigQuery table to track job status and historical records.
CREATE TABLE your_gcp_project_id.your_bq_dataset_id.job_status (
    job_entry_nr INT64 NOT NULL,
    job_kennung STRING NOT NULL,
    stichtag DATE NOT NULL,
    status_code STRING NOT NULL,
    status_text STRING,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);