-- BigQuery Stored Procedure for control logic
-- Replaces: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_bp_ref.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_bp_ref.ksh

CREATE OR REPLACE PROCEDURE `my_project.my_dataset.sp_ausd_v_ta_bp_ref`(
  IN p_JobKennung STRING,
  IN p_EintragsNr INT64
)
BEGIN
  DECLARE v_datum DATE;
  DECLARE v_records_processed INT64;
  DECLARE v_job_status STRING DEFAULT 'FAILED';
  DECLARE v_error_message STRING;
  DECLARE v_current_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP();

  -- Parameter validation
  IF p_JobKennung IS NULL OR TRIM(p_JobKennung) = '' THEN
    RAISE USING MESSAGE = 'Parameter p_JobKennung darf nicht leer sein.';
  END IF;
  IF p_EintragsNr IS NULL THEN
    RAISE USING MESSAGE = 'Parameter p_EintragsNr darf nicht NULL sein.';
  END IF;

  BEGIN
    -- Start transaction for job control table updates and data processing
    BEGIN TRANSACTION;

    -- Deactivate any older active jobs for the same JobKennung
    UPDATE `my_project.my_dataset.job_control_table`
    SET
      status = 'DEACTIVATED',
      end_time = v_current_timestamp,
      error_message = 'Deactivated by newer job instance'
    WHERE
      job_kennung = p_JobKennung
      AND status = 'RUNNING';

    -- Register current job as 'RUNNING'
    INSERT INTO `my_project.my_dataset.job_control_table` (job_kennung, entry_nr, status, start_time)
    VALUES (p_JobKennung, p_EintragsNr, 'RUNNING', v_current_timestamp);

    -- Determine cutoff date (v_datum) from `dwtk_meldungen`
    SET v_datum = (
      SELECT COALESCE(MAX(DATE(m.timecreated)), DATE '1900-01-01')
      FROM `my_project.my_dataset.dwtk_meldungen` AS m
      WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
    );

    IF v_datum IS NULL THEN
      SET v_error_message = 'Cutoff date could not be determined from dwtk_meldungen.';
      RAISE USING MESSAGE = v_error_message;
    END IF;

    -- Call the data processing stored procedure
    CALL `my_project.my_dataset.sp_d_ausd_v_ta_bp_ref`(v_datum, p_EintragsNr, p_JobKennung, v_records_processed);

    -- If the data processing was successful
    SET v_job_status = 'SUCCESS';

    -- Update job control table with success status and processed records
    UPDATE `my_project.my_dataset.job_control_table`
    SET
      status = v_job_status,
      end_time = CURRENT_TIMESTAMP(),
      records_processed = v_records_processed
    WHERE
      job_kennung = p_JobKennung AND entry_nr = p_EintragsNr;

    COMMIT TRANSACTION;

  EXCEPTION WHEN ERROR THEN
    -- Capture error message
    SET v_error_message = @@error.message;

    -- Rollback the transaction
    ROLLBACK TRANSACTION;

    -- Log error to job_error_log and update job_control_table with FAILED status
    -- Re-start transaction for job_control_table update and error logging as previous one was rolled back
    BEGIN TRANSACTION;
    -- Try to update the existing record first, if it was inserted before the error.
    UPDATE `my_project.my_dataset.job_control_table`
    SET
      status = 'FAILED',
      end_time = CURRENT_TIMESTAMP(),
      error_message = v_error_message
    WHERE
      job_kennung = p_JobKennung AND entry_nr = p_EintragsNr;

    -- If the update failed (e.g., record was not inserted due to an error during insertion itself),
    -- then insert a new error record.
    IF (SELECT COUNT(*) FROM `my_project.my_dataset.job_control_table` WHERE job_kennung = p_JobKennung AND entry_nr = p_EintragsNr) = 0 THEN
      INSERT INTO `my_project.my_dataset.job_control_table` (job_kennung, entry_nr, status, start_time, end_time, error_message)
      VALUES (p_JobKennung, p_EintragsNr, 'FAILED', v_current_timestamp, CURRENT_TIMESTAMP(), v_error_message);
    END IF;

    INSERT INTO `my_project.my_dataset.job_error_log` (job_kennung, entry_nr, error_level, error_message)
    VALUES (p_JobKennung, p_EintragsNr, 'ERROR', CONCAT('Job execution failed in sp_ausd_v_ta_bp_ref: ', v_error_message));
    COMMIT TRANSACTION;

    RAISE; -- Re-raise the error to propagate it
  END;
END;