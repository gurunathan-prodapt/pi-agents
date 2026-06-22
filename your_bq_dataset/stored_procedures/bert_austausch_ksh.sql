-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_austausch.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_austausch.ksh
CREATE OR REPLACE PROCEDURE `your_gcp_project.your_bq_dataset.BERT_AUSTAUSCH_KSH`(
    p_stichtag_in STRING DEFAULT NULL, -- Optional Stichtag in DDMMYYYY
    p_wiederanlaufWert_in INT64 DEFAULT 0 -- Optional Wiederanlaufwert
)
BEGIN
    DECLARE v_job_id STRING;
    DECLARE v_stichtag STRING;
    DECLARE v_wiederanlaufwert INT64;
    DECLARE v_job_status STRING;
    DECLARE v_error_message STRING;

    SET v_job_id = GENERATE_UUID();

    -- Parameter defaulting logic
    SET v_wiederanlaufwert = IFNULL(p_wiederanlaufWert_in, 0);
    SET v_stichtag = IFNULL(p_stichtag_in, FORMAT_DATE('%d%m%Y', CURRENT_DATE()));

    -- Log job start
    INSERT INTO `your_gcp_project.your_bq_dataset.job_log` (
        job_id, job_name, start_time, status, message, stichtag_param, wiederanlaufwert_param
    ) VALUES (
        v_job_id, 'BERT_AUSTAUSCH_KSH', CURRENT_TIMESTAMP(), 'RUNNING', 'Main orchestration procedure started', v_stichtag, v_wiederanlaufwert
    );

    BEGIN
        -- Call the core data preparation procedure
        CALL `your_gcp_project.your_bq_dataset.k_ausd_austausch`(
            v_job_id, -- p_jobkennung
            v_stichtag, -- p_stichtag
            '0',        -- p_eintragsnr (placeholder, as its use was unclear)
            v_wiederanlaufwert -- p_wiederanlaufWert
        );

        SET v_job_status = 'SUCCESS';
        SET v_error_message = 'Main orchestration procedure completed successfully.';

    EXCEPTION WHEN ERROR THEN
        SET v_job_status = 'FAILED';
        SET v_error_message = @@error.message;
    END;

    -- Log job end/status
    INSERT INTO `your_gcp_project.your_bq_dataset.job_log` (
        job_id, job_name, end_time, status, message
    ) VALUES (
        v_job_id, 'BERT_AUSTAUSCH_KSH', CURRENT_TIMESTAMP(), v_job_status, v_error_message
    );

    -- Update job_status table
    MERGE INTO `your_gcp_project.your_bq_dataset.job_status` AS T
    USING (SELECT v_job_id AS job_id,
                  'BERT_AUSTAUSCH_KSH' AS job_name,
                  v_job_status AS last_run_status,
                  CURRENT_TIMESTAMP() AS last_run_time,
                  CASE WHEN v_job_status = 'SUCCESS' THEN CURRENT_TIMESTAMP() ELSE T.last_success_time END AS last_success_time,
                  CASE WHEN v_job_status = 'SUCCESS' THEN v_stichtag ELSE T.last_stichtag END AS last_stichtag
          ) AS S
    ON T.job_name = S.job_name
    WHEN MATCHED THEN
        UPDATE SET
            job_id = S.job_id,
            last_run_status = S.last_run_status,
            last_run_time = S.last_run_time,
            last_success_time = S.last_success_time,
            last_stichtag = S.last_stichtag
    WHEN NOT MATCHED THEN
        INSERT (job_id, job_name, last_run_status, last_run_time, last_success_time, last_stichtag)
        VALUES (S.job_id, S.job_name, S.last_run_status, S.last_run_time, S.last_success_time, S.last_stichtag);

    IF v_job_status = 'FAILED' THEN
        RAISE; -- Re-raise the error for external orchestration tools
    END IF;
END;