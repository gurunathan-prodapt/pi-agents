-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_acc.ksh
-- Target: BigQuery Stored Procedure for the wrapper script
CREATE OR REPLACE PROCEDURE `project.dataset.Vertragsdatenabgleich`(
  p_h BOOL,
  p_s STRING,
  p_l STRING
)
BEGIN
  -- ===========================================================
  -- Procedure: project.dataset.Vertragsdatenabgleich
  -- Purpose  : Wrapper script for data reconciliation process of ta_inv_acc.
  --            Handles environment setup, parameter parsing, logging,
  --            and error handling before invoking the core logic.
  -- Params   :
  --   p_h BOOL    - If TRUE, display help message and exit.
  --   p_s STRING  - Placeholder parameter (unused in original script).
  --   p_l STRING  - Placeholder parameter (unused in original script).
  -- ===========================================================

  DECLARE job_id STRING DEFAULT GENERATE_UUID();
  DECLARE start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
  DECLARE v_date_format STRING DEFAULT FORMAT_DATE('%Y%m%d', CURRENT_DATE());
  DECLARE v_stichtag_info STRING DEFAULT 'DEFAULT_STICHTAG_INFO'; -- Placeholder for specific business logic
  DECLARE v_parameters_json STRING;
  DECLARE v_help_message STRING DEFAULT '''
Help: project.dataset.Vertragsdatenabgleich
This procedure orchestrates the reconciliation process for ta_inv_acc table.

Parameters:
  p_h BOOL    - If TRUE, display this help message and exit.
  p_s STRING  - Placeholder parameter for future use (currently unused).
  p_l STRING  - Placeholder parameter for future use (currently unused).

Behavior:
  - Initializes job audit and logging.
  - Sets environment variables/placeholders.
  - Calls the core reconciliation logic: project.dataset.k_ausd_v_ta_inv_acc.
  - Logs success or failure status to audit tables.
''';

  SET v_parameters_json = TO_JSON_STRING(STRUCT(
    p_h AS p_h,
    p_s AS p_s,
    p_l AS p_l
  ));

  -- ============================================================
  -- Job Initialization
  -- ============================================================
  INSERT INTO `project.dataset.job_audit` (
    job_id,
    start_timestamp,
    status,
    source_job_name,
    parameters
  )
  VALUES (
    job_id,
    start_time,
    'RUNNING',
    'r_ausd_v_ta_inv_acc.ksh',
    v_parameters_json
  );

  INSERT INTO `project.dataset.job_log` (
    job_id,
    log_timestamp,
    log_level,
    message,
    component
  )
  VALUES (
    job_id,
    CURRENT_TIMESTAMP(),
    'INFO',
    'Job started: project.dataset.Vertragsdatenabgleich',
    'wrapper'
  );

  -- ============================================================
  -- Help Message Handling
  -- ============================================================
  IF p_h THEN
    SELECT v_help_message AS help_message;

    UPDATE `project.dataset.job_audit`
    SET
      status = 'SUCCESS',
      message = 'Help message displayed; job exited successfully',
      end_timestamp = CURRENT_TIMESTAMP()
    WHERE job_id = job_id; -- Refers to the local variable 'job_id' and the column 'job_id'

    INSERT INTO `project.dataset.job_log` (
      job_id,
      log_timestamp,
      log_level,
      message,
      component
    )
    VALUES (
      job_id,
      CURRENT_TIMESTAMP(),
      'INFO',
      'Help message displayed; job exited successfully',
      'wrapper'
    );

    RETURN;
  END IF;

  -- ============================================================
  -- Environment Setup (Mock/Placeholder)
  -- Replaces shell environment sourcing (. $HOME/.dw_init)
  -- and `typeset -u` (which is not directly applicable in BQSQL)
  -- ============================================================
  INSERT INTO `project.dataset.job_log` (
    job_id,
    log_timestamp,
    log_level,
    message,
    component
  )
  VALUES (
    job_id,
    CURRENT_TIMESTAMP(),
    'INFO',
    'Environment setup started (mock placeholder for .dw_init and shell utilities).',
    'wrapper'
  );

  INSERT INTO `project.dataset.job_log` (
    job_id,
    log_timestamp,
    log_level,
    message,
    component
  )
  VALUES (
    job_id,
    CURRENT_TIMESTAMP(),
    'INFO',
    CONCAT('v_date_format set to ', v_date_format),
    'wrapper'
  );

  INSERT INTO `project.dataset.job_log` (
    job_id,
    log_timestamp,
    log_level,
    message,
    component
  )
  VALUES (
    job_id,
    CURRENT_TIMESTAMP(),
    'INFO',
    CONCAT('v_stichtag_info set to ', v_stichtag_info),
    'wrapper'
  );

  -- ============================================================
  -- Core Logic Invocation with Error Handling
  -- This replaces the invocation of k_ausd_v_ta_inv_acc.ksh
  -- and BigQuery's BEGIN...EXCEPTION WHEN ERROR...END replaces shell 'trap'
  -- ============================================================
  BEGIN
    INSERT INTO `project.dataset.job_log` (
      job_id,
      log_timestamp,
      log_level,
      message,
      component
    )
    VALUES (
      job_id,
      CURRENT_TIMESTAMP(),
      'INFO',
      'Calling project.dataset.k_ausd_v_ta_inv_acc',
      'wrapper'
    );

    CALL `project.dataset.k_ausd_v_ta_inv_acc`(job_id, v_date_format, v_stichtag_info);

    INSERT INTO `project.dataset.job_log` (
      job_id,
      log_timestamp,
      log_level,
      message,
      component
    )
    VALUES (
      job_id,
      CURRENT_TIMESTAMP(),
      'INFO',
      'Returned from project.dataset.k_ausd_v_ta_inv_acc successfully',
      'wrapper'
    );

  EXCEPTION WHEN ERROR THEN
    INSERT INTO `project.dataset.job_error_log` (
      job_id,
      error_timestamp,
      error_message,
      error_details,
      component
    )
    VALUES (
      job_id,
      CURRENT_TIMESTAMP(),
      @@error.message,
      @@error.stack_trace,
      'wrapper'
    );

    UPDATE `project.dataset.job_audit`
    SET
      status = 'FAILED',
      end_timestamp = CURRENT_TIMESTAMP(),
      message = CONCAT('Job failed: ', @@error.message)
    WHERE job_id = job_id; -- Refers to the local variable 'job_id' and the column 'job_id'

    RAISE; -- Re-raises the caught exception to propagate it to the caller
  END;

  -- ============================================================
  -- Job Completion (if no error occurred)
  -- ============================================================
  UPDATE `project.dataset.job_audit`
  SET
    status = 'SUCCESS',
    end_timestamp = CURRENT_TIMESTAMP(),
    message = 'Job completed successfully'
  WHERE job_id = job_id; -- Refers to the local variable 'job_id' and the column 'job_id'

  INSERT INTO `project.dataset.job_log` (
    job_id,
    log_timestamp,
    log_level,
    message,
    component
  )
  VALUES (
    job_id,
    CURRENT_TIMESTAMP(),
    'INFO',
    'Job completed successfully',
    'wrapper'
  );
END;