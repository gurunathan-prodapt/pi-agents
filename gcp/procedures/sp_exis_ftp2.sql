-- Create Target Dataset if it does not exist
CREATE SCHEMA IF NOT EXISTS `your_project_id.isccr_exporter_dataset`
OPTIONS(
  location="us"
);

-- Create reusable Audit Log Table mimicking the standard legacy message framework
CREATE TABLE IF NOT EXISTS `your_project_id.isccr_exporter_dataset.ccr_audit_log` (
  eintrags_nr STRING NOT NULL OPTIONS(description="Process logging entry run identifier"),
  log_timestamp TIMESTAMP NOT NULL OPTIONS(description="Standard timestamp of trace entry"),
  severity STRING NOT NULL OPTIONS(description="Log severity levels: DEBUG, INFO, WARN, ERROR"),
  message STRING NOT NULL OPTIONS(description="Auditable message trace content")
);

-- =================================================================================
-- SUB-PROCEDURE: sp_log_message
-- Purpose: Replaces h_alis_meldungen.ksh (CCRMSG_LogDebug/LogInfo) to isolate logs.
-- =================================================================================
CREATE OR REPLACE PROCEDURE `your_project_id.isccr_exporter_dataset.sp_log_message`(
  IN p_EintragsNr STRING,
  IN p_Severity STRING,
  IN p_Message STRING,
  IN p_Debug INT64
)
BEGIN
  -- If severity is 'DEBUG' and debugging is disabled (not 1), skip logging
  IF p_Severity = 'DEBUG' AND COALESCE(p_Debug, 0) != 1 THEN
    RETURN;
  END IF;

  -- Write message directly to console stream (Query Results)
  SELECT FORMAT("[%s] - Run %s - %s", p_Severity, p_EintragsNr, p_Message) AS runtime_trace_log;

  -- Persist log structurally in audit table
  INSERT INTO `your_project_id.isccr_exporter_dataset.ccr_audit_log` (
    eintrags_nr,
    log_timestamp,
    severity,
    message
  )
  VALUES (
    p_EintragsNr,
    CURRENT_TIMESTAMP(),
    p_Severity,
    p_Message
  );
END;
/

-- =================================================================================
-- EXTERNAL FUNCTION DEFINITION
-- Purpose: Binds BigQuery to the Google Cloud Function.
-- Note: Credentials should be handled within Secret Manager inside the Cloud Function.
-- =================================================================================
CREATE OR REPLACE EXTERNAL FUNCTION `your_project_id.isccr_exporter_dataset.ext_ftp_transfer_handler`(
  file_path STRING,
  temp_file_path STRING,
  server STRING,
  username STRING,
  secret_manager_password_key STRING, -- Pass secret path (e.g. "ftp-pass-key") instead of raw text
  directory STRING,
  action_type STRING
)
RETURNS JSON
OPTIONS (
  endpoint = 'https://us-central1-your_project_id.cloudfunctions.net/ftp-transfer-adapter',
  connection = 'your_project_id.us.ftp_connection'
);
/

-- =================================================================================
-- MAIN ORCHESTRATION PROCEDURE: sp_exis_ftp2
-- Purpose: Executes secure file upload using temporary suffix and renaming.
-- =================================================================================
CREATE OR REPLACE PROCEDURE `your_project_id.isccr_exporter_dataset.sp_exis_ftp2`(
  IN p_EintragsNr STRING,
  IN p_Datei STRING,
  IN p_Server STRING,
  IN p_User STRING,
  IN p_SecretKey STRING, -- Secure reference to Cloud Secret Manager key instead of raw text
  IN p_Verzeichnis STRING,
  IN p_Endung STRING,
  IN p_Debug INT64,
  OUT v_Error INT64
)
BEGIN
  -- Constants mapping directly to target shell configurations
  DECLARE k_FehlerShell INT64 DEFAULT 1;
  DECLARE k_FertigOK INT64 DEFAULT 0;

  -- Local state variables
  DECLARE v_TempDatei STRING;
  DECLARE v_Response JSON;
  DECLARE v_Status STRING;
  DECLARE v_LogMsg STRING;

  -- Initialize states
  SET v_TempDatei = CONCAT(p_Datei, '.tmp');
  SET v_Error = k_FertigOK;

  -- Phase 0: Begin audit trace
  CALL `your_project_id.isccr_exporter_dataset.sp_log_message`(
    p_EintragsNr,
    'DEBUG',
    FORMAT("Uebertrage %s nach %s (Temp: %s)", p_Datei, p_Verzeichnis, v_TempDatei),
    p_Debug
  );

  -- Phase 1: Upload source file to the remote target as a .tmp file
  BEGIN
    SET v_Response = `your_project_id.isccr_exporter_dataset.ext_ftp_transfer_handler`(
      p_Datei,
      v_TempDatei,
      p_Server,
      p_User,
      p_SecretKey,
      p_Verzeichnis,
      'SEND'
    );
    
    -- Extract and parse JSON return payload
    SET v_Status = JSON_VALUE(v_Response.status);
    
    IF v_Status != 'SUCCESS' THEN
      SET v_Error = k_FehlerShell;
      SET v_LogMsg = COALESCE(JSON_VALUE(v_Response.message), "Unbekannter Fehler bei Phase 1.");
    END IF;

  EXCEPTION WHEN ERROR THEN
    -- Resilient error block trapping connection timeouts/network issues safely
    SET v_Error = k_FehlerShell;
    SET v_LogMsg = FORMAT("Exception waehrend Dateisendung abgefangen: %s", @@error.message);
  END;

  -- Halt execution and clean up if Phase 1 failed
  IF v_Error != k_FertigOK THEN
    CALL `your_project_id.isccr_exporter_dataset.sp_log_message`(
      p_EintragsNr,
      'ERROR',
      FORMAT("Konnte %s nicht uebertragen. Details: %s", p_Datei, v_LogMsg),
      p_Debug
    );
    RETURN;
  END IF;

  -- Phase 2: Remote file rename back to final name target configuration
  BEGIN
    SET v_Response = `your_project_id.isccr_exporter_dataset.ext_ftp_transfer_handler`(
      v_TempDatei,
      p_Datei,
      p_Server,
      p_User,
      p_SecretKey,
      p_Verzeichnis,
      'RENAME'
    );
    
    SET v_Status = JSON_VALUE(v_Response.status);
    
    IF v_Status != 'SUCCESS' THEN
      SET v_Error = k_FehlerShell;
      SET v_LogMsg = COALESCE(JSON_VALUE(v_Response.message), "Unbekannter Fehler bei Phase 2.");
    END IF;

  EXCEPTION WHEN ERROR THEN
    SET v_Error = k_FehlerShell;
    SET v_LogMsg = FORMAT("Exception waehrend Datei-Umbenennung abgefangen: %s", @@error.message);
  END;

  -- Verify and complete transaction
  IF v_Error != k_FertigOK THEN
    CALL `your_project_id.isccr_exporter_dataset.sp_log_message`(
      p_EintragsNr,
      'ERROR',
      FORMAT("Konnte %s nicht umbenennen. Details: %s", v_TempDatei, v_LogMsg),
      p_Debug
    );
    RETURN;
  END IF;

  -- Log process execution completion success
  CALL `your_project_id.isccr_exporter_dataset.sp_log_message`(
    p_EintragsNr,
    'INFO',
    FORMAT("Transfer abgeschlossen fuer Datei: %s", p_Datei),
    p_Debug
  );

END;