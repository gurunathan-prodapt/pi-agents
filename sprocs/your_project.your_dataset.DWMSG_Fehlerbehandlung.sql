-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_discount_rr.ksh
-- BigQuery Stored Procedure to handle error conditions and log them.
CREATE OR REPLACE PROCEDURE `your_project.your_dataset.DWMSG_Fehlerbehandlung`(
    IN p_job_key STRING,
    IN p_exit_code INT64,
    IN p_error_message STRING
)
BEGIN
    CALL `your_project.your_dataset.DWMSG_MeldeFehler`(
        p_job_key,
        CONCAT('Job failed with exit code ', p_exit_code, '. Details: ', p_error_message)
    );
    -- Optionally, update the status of the initial 'RUNNING' entry to 'FAILED'
    UPDATE `your_project.your_dataset.job_logging_table`
    SET status = 'FAILED'
    WHERE job_key = p_job_key AND status = 'RUNNING';
END;