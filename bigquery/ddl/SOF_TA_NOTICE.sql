-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_notice.ksh
-- Description: BigQuery DDL for the SOF_TA_NOTICE target table.
-- NOTE: Schema is inferred; please review and adjust based on actual source system schema.
CREATE TABLE IF NOT EXISTS `mydataset.SOF_TA_NOTICE` (
    id STRING NOT NULL,
    job_kennung STRING,
    eintrags_nr STRING,
    data STRING,
    processed_at TIMESTAMP
)
OPTIONS(
    description="Migrated target table for SOF$TA_NOTICE."
);