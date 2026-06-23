-- Target for: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount_rr.ksh
-- Description: DDL for BigQuery audit table to store 'Stichtag' (key date) information for jobs.
CREATE TABLE IF NOT EXISTS project.dataset.job_stichtag (
    job_kennung STRING NOT NULL OPTIONS(description="Unique identifier for the job, e.g., 'BERT_V_TA_P_DISCOUNT_RR'"),
    entry_nr INT64 NOT NULL OPTIONS(description="Sequential entry number for the job run"),
    log_timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the stichtag was set"),
    stichtag DATE NOT NULL OPTIONS(description="The 'Stichtag' or key date for the job execution")
)
OPTIONS(
    description="Table to store 'Stichtag' (key date) information for job runs."
);