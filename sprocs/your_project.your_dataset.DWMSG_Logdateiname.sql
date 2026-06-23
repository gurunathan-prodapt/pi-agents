-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_discount_rr.ksh
-- BigQuery Stored Procedure to determine the conceptual log file name.
CREATE OR REPLACE PROCEDURE `your_project.your_dataset.DWMSG_Logdateiname`(
    IN p_job_key STRING,
    OUT p_log_file_name STRING
)
BEGIN
    SET p_log_file_name = CONCAT(p_job_key, '_', FORMAT_TIMESTAMP('%Y%m%d_%H%M%S', CURRENT_TIMESTAMP()), '.log');
END;