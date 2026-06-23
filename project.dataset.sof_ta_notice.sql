--
-- Target BigQuery DDL for table sof_ta_notice
-- Replaces usage in vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_notice.ksh
--
CREATE OR REPLACE TABLE `project.dataset.sof_ta_notice`
(
    cntrct_id STRING,
    valid_from DATE,
    valid_to DATE,
    entry_date_of_notice DATE
);