-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_cntrct_evn.ksh
-- Purpose: Serves as the entry point, handling parameter validation, defaulting, and orchestrating the call to the core logic.
CREATE OR REPLACE PROCEDURE `project.dataset.ausd_bp_ta_cntrct_evn`(
  IN p_stichtag STRING,
  IN p_wiederanlaufWert INT64
)
BEGIN
  DECLARE v_job_nr INT64 DEFAULT UNIX_MICROS(CURRENT_TIMESTAMP());
  DECLARE v_job_name STRING DEFAULT 'ausd_bp_ta_cntrct_evn';
  DECLARE v_stichtag STRING DEFAULT p_stichtag;
  DECLARE v_restart_value INT64 DEFAULT IFNULL(p_wiederanlaufWert, 0);
  DECLARE v_stichtag_date DATE;

  -- Default stichtag to current date if not provided and validate
  CALL `project.dataset.sp_validate_stichtag`(v_stichtag, v_stichtag_date);
  SET v_stichtag = FORMAT_DATE('%d%m%Y', v_stichtag_date); -- Update v_stichtag with the normalized string format

  -- Log start
  CALL `project.dataset.sp_log_job_event`(
    v_job_nr,
    v_job_name,
    'START',
    v_stichtag,
    v_restart_value,
    'Wrapper procedure started'
  );

  BEGIN
    -- Call core logic
    CALL `project.dataset.k_ausd_bp_ta_cntrct_evn`(
      v_job_name,
      v_stichtag,
      v_job_nr,
      v_restart_value
    );

    -- Log success
    CALL `project.dataset.sp_log_job_event`(
      v_job_nr,
      v_job_name,
      'SUCCESS',
      v_stichtag,
      v_restart_value,
      'Wrapper procedure completed successfully'
    );

  EXCEPTION WHEN ERROR THEN
    CALL `project.dataset.sp_log_job_event`(
      v_job_nr,
      v_job_name,
      'ERROR',
      v_stichtag,
      v_restart_value,
      CONCAT('Wrapper procedure failed: ', @@error.message)
    );
    SIGNAL SQLSTATE '45000'
      SET MESSAGE = CONCAT('Wrapper procedure failed: ', @@error.message);
  END;
END;