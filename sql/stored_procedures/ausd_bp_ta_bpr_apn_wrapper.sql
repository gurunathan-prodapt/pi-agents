-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_apn.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_apn.ksh

CREATE OR REPLACE PROCEDURE `project.dataset.ausd_bp_ta_bpr_apn_wrapper`(
  IN p_stichtag STRING,
  IN p_wiederanlaufWert INT64
)
BEGIN
  DECLARE v_sysdate DATE DEFAULT CURRENT_DATE();
  DECLARE v_stichtag DATE;
  DECLARE v_wiederanlaufWert INT64 DEFAULT 0;
  DECLARE v_job_kennung STRING DEFAULT 'ausd_bp_ta_bpr_apn';
  DECLARE v_job_nr INT64;
  DECLARE v_logdatei STRING;
  DECLARE v_errnr INT64 DEFAULT 0;
  DECLARE v_errarg STRING DEFAULT '';
  DECLARE v_status STRING DEFAULT 'INIT';

  BEGIN
    -- Initialize restart value
    IF p_wiederanlaufWert IS NULL THEN
      SET v_wiederanlaufWert = 0;
    ELSE
      SET v_wiederanlaufWert = p_wiederanlaufWert;
    END IF;

    -- Determine stichtag
    IF p_stichtag IS NULL OR TRIM(p_stichtag) = '' THEN
      SET v_stichtag = v_sysdate;
    ELSE
      SET v_stichtag = PARSE_DATE('%d%m%Y', p_stichtag);
    END IF;

    -- Validate required parameter
    IF v_stichtag IS NULL THEN
      SET v_errnr = 193;
      SET v_errarg = 'Stichtag';
      RAISE USING MESSAGE = 'Required parameter Stichtag missing or invalid';
    END IF;

    -- Create job metadata and log initial status
    SET v_job_nr = (
      SELECT IFNULL(MAX(job_nr), 0) + 1
      FROM `project.dataset.job_control`
    );

    SET v_logdatei = CONCAT('job_', CAST(v_job_nr AS STRING), '_', v_job_kennung, '.log');

    INSERT INTO `project.dataset.job_control`
    (
      job_nr,
      job_kennung,
      script_name,
      log_file,
      sysdate,
      stichtag,
      restart_value,
      status,
      created_at
    )
    VALUES
    (
      v_job_nr,
      v_job_kennung,
      'ausd_bp_ta_bpr_apn_wrapper',
      v_logdatei,
      v_sysdate,
      v_stichtag,
      v_wiederanlaufWert,
      'STARTED',
      CURRENT_TIMESTAMP()
    );

    -- Call the core business logic procedure (placeholder)
    CALL `project.dataset.k_ausd_bp_ta_bpr_apn`( -- This procedure must be implemented
      v_job_kennung,
      FORMAT_DATE('%d%m%Y', v_stichtag),
      v_job_nr,
      v_wiederanlaufWert
    );

    SET v_status = 'OK';

    UPDATE `project.dataset.job_control`
    SET status = v_status,
        finished_at = CURRENT_TIMESTAMP()
    WHERE job_nr = v_job_nr;

  EXCEPTION WHEN ERROR THEN
    UPDATE `project.dataset.job_control`
    SET status = 'ERROR',
        error_message = @@error.message,
        finished_at = CURRENT_TIMESTAMP()
    WHERE job_nr = v_job_nr;

    RAISE USING MESSAGE = CONCAT('AppError: ', @@error.message);
  END;
END;