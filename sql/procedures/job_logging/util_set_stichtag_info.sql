-- Utility procedure to set the 'Stichtag' (reference date) for a job run.
-- Corresponds to legacy DWMSG_SetzeStichtagInfo.
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_action_assoc.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_action_assoc.ksh
CREATE OR REPLACE PROCEDURE project.dataset.util_set_stichtag_info(
    IN p_job_id STRING,
    IN p_stichtag_date DATE
)
BEGIN
    INSERT INTO project.dataset.job_control (
        job_id,
        parameter_name,
        parameter_value,
        description,
        valid_from
    )
    VALUES (
        p_job_id,
        'Stichtag',
        FORMAT_DATE('%Y-%m-%d', p_stichtag_date),
        'Reference date for the contract data reconciliation',
        p_stichtag_date
    );

    CALL project.dataset.util_log_detail(
        p_job_id,
        'INFO',
        FORMAT('Stichtag set to: %s', FORMAT_DATE('%Y-%m-%d', p_stichtag_date)),
        'util_set_stichtag_info'
    );
END;