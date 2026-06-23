-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_carmen.ksh
-- Description: BigQuery DDL for job control log table.
CREATE TABLE IF NOT EXISTS `project.dataset.job_control_log`
(
    tab_name        STRING,
    status_a        STRING,
    status_i        STRING,
    stichtag_from   DATE,
    stichtag_to     DATE,
    job_type        STRING,
    active_flag     STRING,
    record_count    INT64,
    comment_text    STRING,
    job_name        STRING      NOT NULL,
    entry_nr        STRING,
    created_at      TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
OPTIONS(
    description = "Table to record job execution history and metrics."
);