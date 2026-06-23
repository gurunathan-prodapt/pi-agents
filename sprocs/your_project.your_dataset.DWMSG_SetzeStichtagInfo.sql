-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_discount_rr.ksh
-- BigQuery Stored Procedure to log a reference date for the job.
CREATE OR REPLACE PROCEDURE `your_project.your_dataset.DWMSG_SetzeStichtagInfo`(
    IN p_job_key STRING,
    IN p_stichtag_info STRING
)
BEGIN
    CALL `your_project.your_dataset.DWMSG_ErzeugeEintrag`(
        p_job_key,
        CONCAT('Reference Date (Stichtag): ', p_stichtag_info),
        'INFO',
        'RUNNING'
    );
END;