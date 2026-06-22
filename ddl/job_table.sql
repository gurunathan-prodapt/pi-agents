-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_einzeln.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_einzeln.ksh
-- Description: DDL for the BigQuery job audit table.

CREATE TABLE IF NOT EXISTS `project.dataset.job_table` (
    tab_name STRING OPTIONS(description="Name of the table being processed"),
    status_a STRING OPTIONS(description="Status A (e.g., Active)"),
    status_i STRING OPTIONS(description="Status I (e.g., Inactive)"),
    stichtag_from DATE OPTIONS(description="Start date for data processing"),
    stichtag_to DATE OPTIONS(description="End date for data processing (typically the key date)"),
    job_type STRING OPTIONS(description="Type of job (e.g., ETL, Reporting)"),
    restart_flag BOOL OPTIONS(description="Flag indicating if the job was a restart"),
    record_count INT64 OPTIONS(description="Number of records processed or inserted"),
    description STRING OPTIONS(description="Job description"),
    job_kennung STRING OPTIONS(description="Unique job identifier"),
    eintrags_nr STRING OPTIONS(description="Entry number"),
    created_ts TIMESTAMP OPTIONS(description="Timestamp when the job entry was created")
);