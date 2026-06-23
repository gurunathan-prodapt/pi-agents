-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_notice.sql
-- Description: BigQuery DDL for the CDS_TA_NOTICE source table.
-- NOTE: Schema is inferred; please review and adjust based on actual source system schema.
CREATE TABLE IF NOT EXISTS `mydataset.CDS_TA_NOTICE` (
    id STRING NOT NULL,
    data STRING,
    created_at TIMESTAMP
)
OPTIONS(
    description="Migrated source table for CDS_TA_NOTICE."
);