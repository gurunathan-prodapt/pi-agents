-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_drop_temp_table.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_drop_temp_table.ksh
-- Description: BigQuery stored procedure replacing r_drop_temp_table.ksh.
-- This procedure handles parameter parsing, defaulting, logging, and error handling,
-- then calls the core cleanup procedure k_drop_temp_table_core.
CREATE OR REPLACE PROCEDURE `project.dataset.k_drop_temp_table_wrapper`(
  IN p_stichtag_in STRING,
  IN p_wiederanlauf_wert_in INT64
)
BEGIN
  DECLARE v_job_kennung STRING;
  DECLARE v_dw_eintrags_nr INT64;
  DECLARE v_stichtag STRING;
  DECLARE v_wiederanlauf_wert INT64;

  SET v_job_kennung = 'BERT_DROP_TEMP_TABLE';
  -- Generate a unique job entry number. Using FARM_FINGERPRINT of a UUID for simplicity.
  SET v_dw_eintrags_nr = ABS(FARM_FINGERPRINT(GENERATE_UUID()));

  -- Parameter defaulting logic, mimicking the KornShell script
  SET v_wiederanlauf_wert = IFNULL(p_wiederanlauf_wert_in, 0);

  IF p_stichtag_in IS NULL THEN
    -- Default p_stichtag to current date in 'DDMMYYYY' format
    SET v_stichtag = FORMAT_DATE('%d%m%Y', CURRENT_DATE());
  ELSE
    SET v_stichtag = p_stichtag_in;
  END IF;

  -- Initial logging for job start
  INSERT INTO `project.dataset.job_audit_log` (
    job_kennung, job_entry_nr, log_level, message, stichtag, restart_value, created_at
  )
  VALUES (
    v_job_kennung,
    v_dw_eintrags_nr,
    'INFO',
    FORMAT('Job %s started with Stichtag: %s, Wiederanlaufwert: %d', v_job_kennung, v_stichtag, v_wiederanlauf_wert),
    v_stichtag,
    v_wiederanlauf_wert,
    CURRENT_TIMESTAMP()
  );

  -- Main logic with error handling (equivalent to set -e and trap ERR)
  BEGIN
    -- Log parameters passed to the core procedure for auditing/debugging
    INSERT INTO `project.dataset.job_audit_log` (
      job_kennung, job_entry_nr, log_level, message, stichtag, restart_value, created_at
    )
    VALUES (
      v_job_kennung,
      v_dw_eintrags_nr,
      'DEBUG',
      FORMAT('Calling core cleanup with Stichtag: %s, Wiederanlaufwert: %d', v_stichtag, v_wiederanlauf_wert),
      v_stichtag,
      v_wiederanlauf_wert,
      CURRENT_TIMESTAMP()
    );

    -- Call the core stored procedure for dropping temporary tables
    CALL `project.dataset.k_drop_temp_table_core`(
      v_job_kennung,
      v_stichtag,
      v_dw_eintrags_nr,
      v_wiederanlauf_wert
    );

    -- Log successful completion (equivalent to DWMSG_SetzeStatusOK)
    INSERT INTO `project.dataset.job_audit_log` (
      job_kennung, job_entry_nr, log_level, message, stichtag, restart_value, created_at
    )
    VALUES (
      v_job_kennung,
      v_dw_eintrags_nr,
      'INFO',
      'Job completed successfully.',
      v_stichtag,
      v_wiederanlauf_wert,
      CURRENT_TIMESTAMP()
    );

    -- Update job status table
    INSERT INTO `project.dataset.job_status_log` (
      job_kennung, job_entry_nr, status, stichtag, created_at
    )
    VALUES (
      v_job_kennung,
      v_dw_eintrags_nr,
      'OK',
      v_stichtag,
      CURRENT_TIMESTAMP()
    );

  EXCEPTION WHEN ERROR THEN
    -- Log error details (equivalent to DWMSG_Fehlerbehandlung)
    INSERT INTO `project.dataset.job_audit_log` (
      job_kennung, job_entry_nr, log_level, message, stichtag, restart_value, created_at
    )
    VALUES (
      v_job_kennung,
      v_dw_eintrags_nr,
      'ERROR',
      FORMAT('Job failed with error: %s', @@error.message),
      v_stichtag,
      v_wiederanlauf_wert,
      CURRENT_TIMESTAMP()
    );

    -- Update job status table
    INSERT INTO `project.dataset.job_status_log` (
      job_kennung, job_entry_nr, status, stichtag, created_at
    )
    VALUES (
      v_job_kennung,
      v_dw_eintrags_nr,
      'FAILED',
      v_stichtag,
      CURRENT_TIMESTAMP()
    );

    -- Re-raise the error to propagate it to the caller (e.g., Airflow)
    RAISE;
  END;

END;