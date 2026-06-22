--
-- Target BigQuery DDL for job status management table.
-- Replaces implied job management from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs2.ksh
--
CREATE TABLE IF NOT EXISTS `bq_dataset.job_table`
(
    `job_kennung`      STRING      NOT NULL OPTIONS(description="Unique identifier for the job type (e.g., 'TA_CNTRCT_CRS2')"),
    `job_description`  STRING              OPTIONS(description="Descriptive name for the job"),
    `status`           STRING      NOT NULL OPTIONS(description="Current status of the job ('ACTIVE', 'INACTIVE', 'BLOCKED', etc.)"),
    `last_update_time` TIMESTAMP   NOT NULL OPTIONS(description="Timestamp of the last status update"),
    `updated_by`       STRING              OPTIONS(description="User or process that last updated the status")
)
OPTIONS(
    description="Table for managing job activation and status"
);