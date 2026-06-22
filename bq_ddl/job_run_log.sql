--
-- Target BigQuery DDL for job execution logging table.
-- Replaces logging functionality from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs2.ksh
--
CREATE TABLE IF NOT EXISTS `bq_dataset.job_run_log`
(
    `run_id`            STRING      NOT NULL OPTIONS(description="Unique identifier for each job run, e.g., generated UUID"),
    `job_kennung`       STRING      NOT NULL OPTIONS(description="Identifier for the job type (from job_table)"),
    `eintrags_nr`       STRING              OPTIONS(description="Entry number or specific identifier for the run, if applicable"),
    `start_time`        TIMESTAMP   NOT NULL OPTIONS(description="Timestamp when the job run started"),
    `end_time`          TIMESTAMP           OPTIONS(description="Timestamp when the job run ended"),
    `status`            STRING      NOT NULL OPTIONS(description="Final status of the run ('SUCCESS', 'FAILED', 'RUNNING')"),
    `records_processed` INT64               OPTIONS(description="Number of records processed by the job"),
    `error_message`     STRING              OPTIONS(description="Detailed error message if the job failed")
)
OPTIONS(
    description="Log table for BigQuery stored procedure executions and record counts"
);