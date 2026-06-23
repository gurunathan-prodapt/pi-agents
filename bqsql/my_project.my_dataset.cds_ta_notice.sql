-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_notice.ksh
-- This table is a placeholder for the migrated Oracle table 'cds$ta_notice'.
-- Its schema is inferred from the INSERT statement in d_ausd_v_ta_notice.sql.
-- Data for this table should be populated via a separate data ingestion pipeline.
CREATE TABLE IF NOT EXISTS `my_project.my_dataset.cds_ta_notice` (
    cntrct_id STRING,
    valid_from DATE,
    valid_to DATE,
    entry_date_of_notice DATE,
    insert_at DATE,
    modified_at DATE,
    is_production INT64
);