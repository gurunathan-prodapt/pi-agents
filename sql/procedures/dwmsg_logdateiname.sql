-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh
-- Target Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh

-- This stored procedure constructs a standardized log filename string.
-- Replace `my_project.my_dataset` with your actual BigQuery project and dataset IDs.

CREATE OR REPLACE PROCEDURE `my_project.my_dataset.dwmsg_logdateiname`(
    IN p_job_kennung STRING,
    IN p_programm_name STRING,
    OUT p_log_filename STRING
)
BEGIN
    SET p_log_filename = CONCAT(
        p_job_kennung,
        '_',
        p_programm_name,
        '_',
        FORMAT_TIMESTAMP('%Y%m%d_%H%M%S', CURRENT_TIMESTAMP(), 'Europe/Berlin'), -- Assuming a timezone for consistency
        '.log'
    );
END;