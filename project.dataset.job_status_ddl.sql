-- Target for: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount_rr.ksh
-- Description: DDL for BigQuery audit table to store job status updates.
CREATE TABLE IF NOT EXISTS project.dataset.job_status (
    job_kennung STRING NOT NULL OPTIONS(description="Unique identifier for the job, e.g., 'BERT_V_TA_P_DISCOUNT_RR'"),
    entry_nr INT64 NOT NULL OPTIONS(description="Sequential entry number for the job run"),
    log_timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the status was updated"),
    status STRING NOT NULL OPTIONS(description="Status of the job (e.g., 'STARTED', 'COMPLETED', 'FAILED')"),
    message STRING OPTIONS(description="Additional details about the job status")
)
OPTIONS(
    description="Table to store status updates for batch jobs."
);