-- BigQuery Stored Procedure for orchestration logic from k_ausd_v_ta_barrier_zusgf.ksh
-- Original job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_barrier_zusgf.ksh
CREATE OR REPLACE PROCEDURE `your_project_id.your_dataset_id.p_k_ausd_v_ta_barrier_zusgf`(
    IN p_job_kennung STRING,
    IN p_eintrags_nr STRING
)
BEGIN
    DECLARE v_start_time TIMESTAMP;
    DECLARE v_end_time TIMESTAMP;
    DECLARE v_record_count INT64 DEFAULT 0;
    DECLARE v_error_message STRING DEFAULT NULL;
    DECLARE v_status STRING DEFAULT 'FAILED';
    DECLARE v_is_job_active BOOL DEFAULT FALSE;

    SET v_start_time = CURRENT_TIMESTAMP();

    -- Parameter validation
    IF p_job_kennung IS NULL OR TRIM(p_job_kennung) = '' THEN
        SET v_error_message = 'ERROR: p_job_kennung parameter is missing or empty.';
        INSERT INTO `your_project_id.your_dataset_id.job_error_log` (job_kennung, eintrags_nr, error_code, error_message, severity)
        VALUES (p_job_kennung, p_eintrags_nr, 193, v_error_message, 'ERROR');
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
    END IF;

    IF p_eintrags_nr IS NULL OR TRIM(p_eintrags_nr) = '' THEN
        SET v_error_message = 'ERROR: p_eintrags_nr parameter is missing or empty.';
        INSERT INTO `your_project_id.your_dataset_id.job_error_log` (job_kennung, eintrags_nr, error_code, error_message, severity)
        VALUES (p_job_kennung, p_eintrags_nr, 193, v_error_message, 'ERROR');
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_error_message;
    END IF;

    -- Check for an actively running job instance with the same job_kennung and eintrags_nr
    SELECT COUNT(*) > 0 INTO v_is_job_active
    FROM `your_project_id.your_dataset_id.job_control`
    WHERE job_kennung = p_job_kennung
      AND eintrags_nr = p_eintrags_nr
      AND status = 'RUNNING';

    IF v_is_job_active THEN
        SET v_error_message = FORMAT("WARNING: Job with job_kennung '%s' and eintrags_nr '%s' is already active. Ignoring this run.", p_job_kennung, p_eintrags_nr);
        INSERT INTO `your_project_id.your_dataset_id.job_error_log` (job_kennung, eintrags_nr, error_code, error_message, severity)
        VALUES (p_job_kennung, p_eintrags_nr, 0, v_error_message, 'WARNING');
        RETURN; -- Exit gracefully as per original ksh logic for active jobs
    END IF;

    -- Deactivate any previously running jobs for this job_kennung that might not have completed successfully
    UPDATE `your_project_id.your_dataset_id.job_control`
    SET
        status = 'INACTIVE',
        end_time = v_start_time,
        error_message = 'Deactivated by new job instance'
    WHERE job_kennung = p_job_kennung
      AND status = 'RUNNING';

    -- Register job start in job_control table
    INSERT INTO `your_project_id.your_dataset_id.job_control` (job_kennung, eintrags_nr, start_time, status)
    VALUES (p_job_kennung, p_eintrags_nr, v_start_time, 'RUNNING');

    BEGIN
        -- Execute the core data processing logic
        CALL `your_project_id.your_dataset_id.p_d_ausd_v_ta_barrier_zusgf`();

        -- Get the number of records processed/inserted
        SELECT COUNT(*) INTO v_record_count
        FROM `your_project_id.your_dataset_id.SOF_TA_BARRIER_ZUSGF`;

        SET v_status = 'SUCCESS';

    EXCEPTION WHEN ERROR THEN
        SET v_error_message = @@error.message;
        SET v_status = 'FAILED';
        INSERT INTO `your_project_id.your_dataset_id.job_error_log` (job_kennung, eintrags_nr, error_code, error_message, severity)
        VALUES (p_job_kennung, p_eintrags_nr, NULL, v_error_message, 'ERROR');
    END;

    SET v_end_time = CURRENT_TIMESTAMP();

    -- Update job_control table with final status and record count
    UPDATE `your_project_id.your_dataset_id.job_control`
    SET
        end_time = v_end_time,
        status = v_status,
        record_count = v_record_count,
        error_message = v_error_message
    WHERE job_kennung = p_job_kennung
      AND eintrags_nr = p_eintrags_nr
      AND start_time = v_start_time; -- Ensure we update the specific job instance started above

END;