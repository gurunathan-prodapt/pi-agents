-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_evn.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_evn.ksh

CREATE OR REPLACE PROCEDURE `your_gcp_project.your_bigquery_dataset.ausd_bp_ta_bpr_evn_wrapper`(
  p_stichtag STRING,
  p_wiederanlaufWert INT64
)
BEGIN
  DECLARE v_stichtag_date DATE;
  DECLARE v_wiederanlaufWert_int INT64;

  -- Default / parse input parameters
  SET v_stichtag_date = IF(
    p_stichtag IS NULL OR TRIM(p_stichtag) = '',
    CURRENT_DATE(),
    SAFE.PARSE_DATE('%Y-%m-%d', p_stichtag)
  );

  IF v_stichtag_date IS NULL THEN
    RAISE USING MESSAGE = CONCAT(
      'Invalid p_stichtag format. Expected YYYY-MM-DD, got: ',
      COALESCE(p_stichtag, 'NULL')
    );
  END IF;

  SET v_wiederanlaufWert_int = IFNULL(p_wiederanlaufWert, 0);

  -- Job start log
  INSERT INTO `your_gcp_project.your_bigquery_dataset.job_audit_log` (
    job_name,
    log_ts,
    message,
    stichtag,
    restart_value,
    status
  )
  VALUES (
    'r_ausd_bp_ta_bpr_evn',
    CURRENT_TIMESTAMP(),
    'Job started',
    v_stichtag_date,
    v_wiederanlaufWert_int,
    'STARTED'
  );

  BEGIN
    -- Call downstream stored procedure
    CALL `your_gcp_project.your_bigquery_dataset.ausd_bp_ta_bpr_evn_core`(
      v_stichtag_date,
      v_wiederanlaufWert_int
    );

    -- Job success log
    INSERT INTO `your_gcp_project.your_bigquery_dataset.job_audit_log` (
      job_name,
      log_ts,
      message,
      stichtag,
      restart_value,
      status
    )
    VALUES (
      'r_ausd_bp_ta_bpr_evn',
      CURRENT_TIMESTAMP(),
      'Job completed successfully',
      v_stichtag_date,
      v_wiederanlaufWert_int,
      'SUCCESS'
    );

  EXCEPTION WHEN ERROR THEN
    -- Job failure log
    INSERT INTO `your_gcp_project.your_bigquery_dataset.job_audit_log` (
      job_name,
      log_ts,
      message,
      stichtag,
      restart_value,
      status
    )
    VALUES (
      'r_ausd_bp_ta_bpr_evn',
      CURRENT_TIMESTAMP(),
      CONCAT('Job failed with error: ', ERROR_MESSAGE()),
      v_stichtag_date,
      v_wiederanlaufWert_int,
      'FAILED'
    );

    -- Re-raise the error
    RAISE;
  END;
END;