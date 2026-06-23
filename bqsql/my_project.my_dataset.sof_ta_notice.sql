-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_notice.ksh
-- This table is the target table 'sof$ta_notice' in BigQuery.
-- Its schema is inferred from the INSERT statement in d_ausd_v_ta_notice.sql.
CREATE TABLE IF NOT EXISTS `my_project.my_dataset.sof_ta_notice` (
    cntrct_id STRING,
    valid_from DATE,
    valid_to DATE,
    entry_date_of_notice DATE
);