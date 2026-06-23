-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_notice.ksh
-- Description: BigQuery DDL for the job control table, replacing legacy job state management.
CREATE TABLE IF NOT EXISTS `mydataset.job_table` (
    job_id STRING NOT NULL OPTIONS(description="Unique identifier for the job run, typically a UUID or timestamp-based ID."),
    job_kennung STRING NOT NULL OPTIONS(description="Corresponds to legacy JobKennung parameter (-j)."),
    eintrags_nr STRING NOT NULL OPTIONS(description="Corresponds to legacy EintragsNr parameter (-f)."),
    table_name STRING NOT NULL OPTIONS(description="Name of the table being processed by the job (e.g., 'ta_notice')."),
    status STRING NOT NULL OPTIONS(description="Current status of the job (ACTIVE, DEACTIVATED, COMPLETED, FAILED)."),
    start_time TIMESTAMP OPTIONS(description="Timestamp when the job started."),
    end_time TIMESTAMP OPTIONS(description="Timestamp when the job completed or failed."),
    record_count INT64 OPTIONS(description="Number of records processed or inserted by the job."),
    message STRING OPTIONS(description="Additional status or error message.")
)
OPTIONS(
    description="Table to manage the state and history of ETL jobs."
);