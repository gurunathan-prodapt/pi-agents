-- Target for legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_discount.ksh
-- Description: BigQuery stored procedure to provide a conceptual log file name (based on job_kennung).
CREATE OR REPLACE PROCEDURE your_gcp_project_id.your_bq_dataset_id.dwmsg_logdateiname_proc(
    IN p_job_kennung STRING,
    OUT p_log_filename STRING
)
BEGIN
    SET p_log_filename = CONCAT('job_log_', p_job_kennung, '.log');
END;