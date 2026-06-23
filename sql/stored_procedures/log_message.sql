-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_austausch.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_austausch.ksh

-- Helper procedure for logging messages to the job_log table.

CREATE OR REPLACE PROCEDURE `project.admin_dataset.log_message`(
    job_name STRING,
    run_id STRING,
    level STRING,
    message STRING
)
BEGIN
    INSERT INTO `project.admin_dataset.job_log` (log_time, job_name, run_id, level, message)
    VALUES (CURRENT_TIMESTAMP(), job_name, run_id, level, message);
END;
;