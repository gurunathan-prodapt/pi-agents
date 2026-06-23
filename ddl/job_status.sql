--
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vvl_upgrade.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vvl_upgrade.ksh
--
-- DDL for the job_status table, replacing custom DWMSG job status tracking.
--
CREATE TABLE IF NOT EXISTS `project.dataset.job_status` (
    job_number INT64 NOT NULL,
    job_identifier STRING NOT NULL,
    status STRING NOT NULL,
    stichtag DATE,
    updated_timestamp TIMESTAMP NOT NULL
);