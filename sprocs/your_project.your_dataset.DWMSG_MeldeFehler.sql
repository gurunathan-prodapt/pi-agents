-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_discount_rr.ksh
-- BigQuery Stored Procedure to log error messages.
CREATE OR REPLACE PROCEDURE `your_project.your_dataset.DWMSG_MeldeFehler`(
    IN p_job_key STRING,
    IN p_error_message STRING
)
BEGIN
    CALL `your_project.your_dataset.DWMSG_ErzeugeEintrag`(
        p_job_key,
        CONCAT('ERROR: ', p_error_message),
        'ERROR',
        'FAILED'
    );
END;