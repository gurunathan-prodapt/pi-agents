--
-- BigQuery DDL for the error logging table.
-- Replaces: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_discount.ksh
--
CREATE TABLE IF NOT EXISTS `project.dataset.error_log`
(
    error_number            INT64,
    error_argument          STRING,
    job_kennung             STRING,
    eintrags_nr             STRING,
    created_at              TIMESTAMP NOT NULL,
    error_message           STRING
);