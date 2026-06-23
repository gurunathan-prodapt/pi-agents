-- Utility procedure to log detailed messages for a job run.
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_action_assoc.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_action_assoc.ksh
CREATE OR REPLACE PROCEDURE project.dataset.util_log_detail(
    IN p_job_id STRING,
    IN p_log_level STRING,
    IN p_message STRING,
    IN p_source_procedure STRING
)
BEGIN
    INSERT INTO project.dataset.job_log_detail (
        detail_id,
        job_id,
        timestamp,
        log_level,
        message,
        source_procedure
    )
    VALUES (
        (SELECT IFNULL(MAX(detail_id), 0) + 1 FROM project.dataset.job_log_detail),
        p_job_id,
        CURRENT_TIMESTAMP(),
        p_log_level,
        p_message,
        p_source_procedure
    );
END;