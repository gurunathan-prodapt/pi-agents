-- Utility procedure to create a new job entry and return its ID.
-- Corresponds to legacy DWMSG_ErzeugeEintrag.
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_action_assoc.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_action_assoc.ksh
CREATE OR REPLACE PROCEDURE project.dataset.util_create_job_entry(
    IN p_job_name STRING,
    IN p_job_version STRING,
    IN p_parameters JSON,
    OUT p_job_id STRING
)
BEGIN
    SET p_job_id = GENERATE_UUID();

    INSERT INTO project.dataset.job_log (
        job_id,
        job_name,
        start_time,
        status,
        version,
        parameters_json
    )
    VALUES (
        p_job_id,
        p_job_name,
        CURRENT_TIMESTAMP(),
        'RUNNING',
        p_job_version,
        p_parameters
    );

END;