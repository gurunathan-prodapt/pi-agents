-- Entry-point stored procedure for the contract data reconciliation job.
-- Handles initial parameter validation and invocation of the main orchestration procedure.
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_action_assoc.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_action_assoc.ksh
CREATE OR REPLACE PROCEDURE project.dataset.sp_vertragsdatenabgleich_entry(
    IN p_stichtag_date DATE,
    IN p_job_version STRING DEFAULT '1.0',
    IN p_debug BOOL DEFAULT FALSE
)
BEGIN
    IF p_stichtag_date IS NULL THEN
        RAISE USING MESSAGE = 'Parameter p_stichtag_date must be provided.';
    END IF;

    -- Call the main orchestration procedure
    CALL project.dataset.sp_vertragsdatenabgleich(p_stichtag_date, p_job_version, p_debug);

END;