-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs2.ksh
-- This file defines the job control table schema in BigQuery.

CREATE TABLE IF NOT EXISTS `project.dataset.job_table` (
    job_kennung STRING NOT NULL OPTIONS(description="Job Identifier, e.g., 'TA_CNTRCT_CRS2'"),
    eintrags_nr STRING NOT NULL OPTIONS(description="Entry Number for the job run, typically a timestamp or unique ID"),
    active_flag BOOL NOT NULL OPTIONS(description="TRUE if the job is currently active, FALSE otherwise"),
    created_at TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the job entry was created"),
    updated_at TIMESTAMP OPTIONS(description="Timestamp when the job entry was last updated"),
    PRIMARY KEY (job_kennung, eintrags_nr) NOT ENFORCED
)
OPTIONS(
    description="Table to manage job status, activation, and deactivation for ETL processes."
);