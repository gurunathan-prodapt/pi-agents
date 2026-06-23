--
-- Target BigQuery DDL for table cds_ta_notice
-- Replaces usage in vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_notice.ksh
--
-- NOTE: This is a placeholder schema based on the query usage.
--       The actual schema should be derived from the source Oracle `cds$ta_notice` table.
--
CREATE OR REPLACE TABLE `project.dataset.cds_ta_notice`
(
    cntrct_id STRING,
    valid_from DATE,
    valid_to DATE,
    entry_date_of_notice DATE,
    insert_at DATE,
    modified_at DATE,
    is_production INT64
);