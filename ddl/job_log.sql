--
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vvl_upgrade.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vvl_upgrade.ksh
--
-- DDL for the job_log table, replacing custom DWMSG logging.
--
CREATE TABLE IF NOT EXISTS `project.dataset.job_log` (
    log_timestamp TIMESTAMP NOT NULL,
    job_number INT64,
    job_identifier STRING,
    severity STRING,
    message STRING
);