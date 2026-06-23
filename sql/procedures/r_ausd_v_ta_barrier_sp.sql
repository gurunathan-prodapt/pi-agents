-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_barrier.ksh
-- Description: BigQuery Stored Procedure, migrated from the KornShell wrapper script.
-- This procedure orchestrates the contract data reconciliation job.
CREATE OR REPLACE PROCEDURE `my_project_id.my_dataset_id.r_ausd_v_ta_barrier_sp`(
    p_job_kennung STRING,
    p_s STRING, -- Corresponds to original -s param
    p_l STRING  -- Corresponds to original -l param
)
BEGIN
  DECLARE ProgName STRING DEFAULT 'Vertragsdatenabgleich';
  DECLARE ProgVersion STRING DEFAULT 'V1.0.0';
  DECLARE v_sysdate STRING DEFAULT FORMAT_DATE('%d%m%Y', CURRENT_DATE());
  DECLARE ErrNr INT64 DEFAULT 0;
  DECLARE ErrArg STRING DEFAULT '';
  DECLARE DW_EintragsNr INT64;
  DECLARE LogDatei STRING;
  DECLARE Name_Kernskript STRING DEFAULT 'k_ausd_v_ta_barrier_sp'; -- Name of the migrated kernel SP

  -- --- Parameter Validation (replaces getopts logic) ---
  -- This section would contain logic to validate p_s and p_l parameters
  -- based on their original usage. If invalid, set ErrNr and ErrArg.
  -- Example validation: assuming -s and -l are mandatory
  IF p_s IS NULL OR p_l IS NULL THEN
    SET ErrNr = 193; -- Missing argument (placeholder error code)
    SET ErrArg = 's/l';
  END IF;

  IF ErrNr != 0 THEN
    -- Log the error using the migrated utility procedure
    CALL `my_project_id.my_dataset_id.DWMSG_MeldeFehler_SP`(DW_EintragsNr, 'E', ErrNr, ErrArg);
    SELECT CONCAT('Programm: ', ProgName, '\nVersion:  ', ProgVersion, '\nAufruf:   CALL r_ausd_v_ta_barrier_sp(job_kennung => ''<YOUR_JOB_ID>'', s => ''<VALUE_S>'', l => ''<VALUE_L>'')') AS usage_info;
    RAISE USING MESSAGE = CONCAT('Parameter Error: ', CAST(ErrNr AS STRING), ' - ', ErrArg);
  END IF;

  -- --- Job Initialization ---
  CALL `my_project_id.my_dataset_id.DWMSG_ErmittleNr_SP`(DW_EintragsNr); -- Get a unique job entry number
  CALL `my_project_id.my_dataset_id.DWMSG_Logdateiname_SP`(LogDatei, p_job_kennung, DW_EintragsNr); -- Determine log file equivalent name
  CALL `my_project_id.my_dataset_id.DWMSG_ErzeugeEintrag_SP`(DW_EintragsNr, p_job_kennung, 'BQ_SCRIPT', LogDatei); -- Create initial log entry
  CALL `my_project_id.my_dataset_id.DWMSG_SetzeStichtagInfo_SP`(DW_EintragsNr, v_sysdate, 'DDMMYYYY'); -- Set reference date

  -- --- Job Banner Logging ---
  INSERT INTO `my_project_id.my_dataset_id.job_log_table`(job_nr, job_kennung, log_message, log_ts, severity)
  VALUES (
    DW_EintragsNr,
    p_job_kennung,
    CONCAT('Job-Nr: ', CAST(DW_EintragsNr AS STRING), ', JobKennung: ', p_job_kennung, ', Logdatei: ', LogDatei),
    CURRENT_TIMESTAMP(),
    'INFO'
  );

  -- --- Core Kernel Logic Execution ---
  BEGIN
    CALL `my_project_id.my_dataset_id.k_ausd_v_ta_barrier_sp`(p_job_kennung, DW_EintragsNr); -- Execute the migrated kernel SP
    
    -- --- Success Handling ---
    INSERT INTO `my_project_id.my_dataset_id.job_log_table`(job_nr, job_kennung, log_message, log_ts, severity)
    VALUES (
      DW_EintragsNr,
      p_job_kennung,
      'Die Abarbeitung wurde ohne erkennbare Fehler beendet',
      CURRENT_TIMESTAMP(),
      'INFO'
    );
    CALL `my_project_id.my_dataset_id.DWMSG_SetzeStatusOK_SP`(DW_EintragsNr);

  EXCEPTION WHEN ERROR THEN
    -- --- Error Handling (replaces trap ERR/INT) ---
    CALL `my_project_id.my_dataset_id.DWMSG_Fehlerbehandlung_SP`(DW_EintragsNr);
    INSERT INTO `my_project_id.my_dataset_id.job_log_table`(job_nr, job_kennung, log_message, log_ts, severity)
    VALUES (
      DW_EintragsNr,
      p_job_kennung,
      CONCAT('AppError: Abbruch - ', @@error.message),
      CURRENT_TIMESTAMP(),
      'ERROR'
    );
    RAISE; -- Re-raise the error to the orchestrator
  END;

END;