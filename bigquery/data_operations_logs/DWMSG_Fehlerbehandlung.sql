-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/allgemein/is/util/bin/f_alis_msgerr.ksh
CREATE OR REPLACE PROCEDURE data_operations_logs.DWMSG_Fehlerbehandlung(
  IN p_eintrags_nr STRING,
  IN p_error_code INT64,
  IN p_error_message STRING
)
BEGIN
  IF p_eintrags_nr IS NULL THEN
    RAISE BQEXCEPTION MESSAGE 'DWMSG_Fehlerbehandlung: EintragsNr must be provided.';
  END IF;

  -- Log the error
  CALL data_operations_logs.DWMSG_MeldeFehler(
    p_eintrags_nr,
    'F',             -- Fatal error type
    p_error_code,
    NULL,
    NULL,
    p_error_message
  );

  -- Set status to Aborted
  CALL data_operations_logs.DWMSG_SetzeStatusAbbruch(p_eintrags_nr);

  -- In a real-world scenario, you might add additional logging or notification
  -- mechanisms here, possibly by calling other stored procedures or UDFs.
  -- For instance, to trigger an alert if this procedure is called.

  -- The original KSH script would exit here, but a BigQuery stored procedure
  -- simply completes execution. The orchestrator is responsible for
  -- handling the overall job flow and stopping on error.
END;