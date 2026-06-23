-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_discount_rr.ksh
-- BigQuery Stored Procedure to generate a unique job entry number for logging.
CREATE OR REPLACE PROCEDURE `your_project.your_dataset.DWMSG_ErmittleNr`(
    OUT p_next_job_entry_nr INT64
)
BEGIN
    SELECT IFNULL(MAX(job_entry_nr), 0) + 1 INTO p_next_job_entry_nr
    FROM `your_project.your_dataset.job_logging_table`;
END;