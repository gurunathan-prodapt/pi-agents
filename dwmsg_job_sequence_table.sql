-- DDL for project.dataset.dwmsg_job_sequence
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bcp_iccid.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bcp_iccid.ksh
-- Purpose: Manages job entry numbers for logging.

CREATE TABLE IF NOT EXISTS project.dataset.dwmsg_job_sequence (
    job_name STRING NOT NULL OPTIONS(description="Unique name of the job"),
    current_sequence_value INT64 NOT NULL OPTIONS(description="The current sequence value for the job"),
    last_updated TIMESTAMP NOT NULL OPTIONS(description="Timestamp of the last update to the sequence"),
    PRIMARY KEY (job_name) NOT ENFORCED
)
OPTIONS(
    description="Table to manage job sequence numbers for logging and other purposes."
);