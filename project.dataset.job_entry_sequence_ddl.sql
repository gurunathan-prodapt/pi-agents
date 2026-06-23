-- Target for: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount_rr.ksh
-- Description: DDL for BigQuery table to manage sequential job entry numbers.
CREATE TABLE IF NOT EXISTS project.dataset.job_entry_sequence (
    job_kennung STRING NOT NULL OPTIONS(description="Unique identifier for the job"),
    entry_nr INT64 NOT NULL OPTIONS(description="The last assigned sequential entry number for this job"),
    last_update_timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp of the last update")
)
OPTIONS(
    description="Table to manage sequential entry numbers for job runs, replacing file-based sequence generation."
);