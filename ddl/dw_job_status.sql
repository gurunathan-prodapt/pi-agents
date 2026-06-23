-- DDL for dw_job_status table
-- Replaces job status functionality from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_acc.ksh
CREATE TABLE IF NOT EXISTS `project.dataset.dw_job_status` (
    job_id STRING NOT NULL,
    run_id STRING NOT NULL,
    status STRING,
    last_update_timestamp TIMESTAMP NOT NULL,
    last_message STRING,
    PRIMARY KEY (job_id, run_id) NOT ENFORCED
);