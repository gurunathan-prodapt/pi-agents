-- Target: BigQuery
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount.ksh
-- Description: DDL for BigQuery table to store contextual information for job runs, such as reference dates.

CREATE TABLE IF NOT EXISTS project.dataset.dw_job_context (
    job_kennung STRING NOT NULL OPTIONS(description="Identifier for the specific job type"),
    dw_eintrags_nr INT64 NOT NULL OPTIONS(description="Corresponding entry number from dw_job_log"),
    stichtag DATE NOT NULL OPTIONS(description="Reference date or system date for the job run"),
    creation_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Timestamp when this context entry was created")
)
OPTIONS(
    description="Contextual information for job runs, such as reference dates."
);