-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_bp_ref.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_bp_ref.ksh
-- Description: BigQuery stored procedure replacing the KornShell wrapper script.

CREATE OR REPLACE PROCEDURE `your_project_id.your_dataset_id.vertragsdatenabgleich_wrapper`(
    IN p_s STRING,
    IN p_l STRING,
    IN p_help BOOL
)
OPTIONS(description="Wrapper procedure for contract data reconciliation (ta_bp_ref). Orchestrates the call to the core k_ausd_v_ta_bp_ref procedure.")
BEGIN
    DECLARE v_ProgName STRING DEFAULT 'Vertragsdatenabgleich';
    DECLARE v_ProgVersion STRING DEFAULT 'V1.0.0';
    DECLARE v_JobKennung STRING DEFAULT 'BERT_V_TA_BP_REF';
    DECLARE v_sysdate STRING;
    DECLARE v_job_instance_id STRING;
    DECLARE v_start_timestamp TIMESTAMP;

    -- Set current date in DDMMYYYY format
    SET v_sysdate = FORMAT_DATE('%d%m%Y', CURRENT_DATE());
    SET v_start_timestamp = CURRENT_TIMESTAMP();
    SET v_job_instance_id = GENERATE_UUID();

    -- Simulate usage function as per the original shell script's -h parameter
    IF p_help THEN
        SELECT FORMAT(
            """
            Programm: %s
            Version:  %s
            Aufruf:   CALL `your_project_id.your_dataset_id.vertragsdatenabgleich_wrapper`(p_s => '[value]', p_l => '[value]', p_help => [TRUE/FALSE])
            Parameter:
                p_s    : This parameter corresponds to the original -s option. Its specific use is determined by the core script.
                p_l    : This parameter corresponds to the original -l option. Its specific use is determined by the core script.
                p_help : Set to TRUE to display this usage message.

            Beschreibung:
                Rahmenskript fuer den Abgleich der Vertragsdaten: Tabelle ta_bp_ref.
            """,
            v_ProgName,
            v_ProgVersion
        );
        -- Log usage display
        INSERT INTO `your_project_id.your_dataset_id.job_audit_log` (
            job_instance_id, job_name, job_kennung, start_timestamp, status,
            message_timestamp, message_type, message_text,
            parameters_s, parameters_l
        )
        VALUES (
            v_job_instance_id, v_ProgName, v_JobKennung, v_start_timestamp, 'COMPLETED',
            CURRENT_TIMESTAMP(), 'USAGE', 'Usage information displayed.',
            p_s, p_l
        );
        RETURN;
    END IF;

    -- Initialize the primary audit log entry for this job instance
    INSERT INTO `your_project_id.your_dataset_id.job_audit_log` (
        job_instance_id, job_name, job_kennung, start_timestamp, status,
        message_timestamp, message_type, message_text,
        parameters_s, parameters_l, stichtag_info, log_file_name
    )
    VALUES (
        v_job_instance_id, v_ProgName, v_JobKennung, v_start_timestamp, 'STARTED',
        CURRENT_TIMESTAMP(), 'INFO', 'Job execution started.',
        p_s, p_l, v_sysdate, v_job_instance_id || '.log' -- Simulating a log file name
    );

    -- Main execution block with error handling
    BEGIN
        -- Log job details (similar to original script's print statements)
        INSERT INTO `your_project_id.your_dataset_id.job_audit_log` (
            job_instance_id, message_timestamp, message_type, message_text
        )
        VALUES (
            v_job_instance_id, CURRENT_TIMESTAMP(), 'INFO',
            FORMAT('Job-Nr: %s, JobKennung: %s, Logdatei: %s.log', v_job_instance_id, v_JobKennung, v_job_instance_id)
        );

        -- Placeholder for the core script invocation
        -- The original script runs: ${Name_Kernskript} -j $JobKennung -f ${DW_EintragsNr}
        -- Assuming k_ausd_v_ta_bp_ref BigQuery Stored Procedure takes these parameters.
        CALL `your_project_id.your_dataset_id.k_ausd_v_ta_bp_ref`(v_JobKennung, v_job_instance_id);

        -- If execution reaches here, it means the core script completed successfully
        INSERT INTO `your_project_id.your_dataset_id.job_audit_log` (
            job_instance_id, message_timestamp, message_type, message_text
        )
        VALUES (
            v_job_instance_id, CURRENT_TIMESTAMP(), 'INFO', 'Die Abarbeitung wurde ohne erkennbare Fehler beendet'
        );

        -- Update the job's overall status to SUCCESS
        UPDATE `your_project_id.your_dataset_id.job_audit_log`
        SET status = 'SUCCESS', end_timestamp = CURRENT_TIMESTAMP()
        WHERE job_instance_id = v_job_instance_id AND status = 'STARTED'; -- Only update the initial 'STARTED' row

    EXCEPTION WHEN ERROR THEN
        -- Capture error details
        DECLARE v_error_code INT64 DEFAULT IFNULL(@@error.code, -1);
        DECLARE v_error_message STRING DEFAULT IFNULL(@@error.message, 'Unknown error');

        -- Log the error
        INSERT INTO `your_project_id.your_dataset_id.job_audit_log` (
            job_instance_id, message_timestamp, message_type, message_text,
            error_code, error_argument
        )
        VALUES (
            v_job_instance_id, CURRENT_TIMESTAMP(), 'ERROR',
            FORMAT('Job execution failed: %s', v_error_message),
            v_error_code, v_error_message
        );

        -- Update the job's overall status to FAILED
        UPDATE `your_project_id.your_dataset_id.job_audit_log`
        SET status = 'FAILED', end_timestamp = CURRENT_TIMESTAMP()
        WHERE job_instance_id = v_job_instance_id AND status = 'STARTED'; -- Only update the initial 'STARTED' row

        -- Re-raise the error to ensure the calling context is aware of the failure
        RAISE;
    END;
END;