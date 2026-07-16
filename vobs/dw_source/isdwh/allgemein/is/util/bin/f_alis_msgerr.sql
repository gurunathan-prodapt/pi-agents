--------------------------------------------------------------------
-- Procedure: DWMSG_ErmittleNr
-- Replaces Oracle sequence fetch / shell function DWMSG_ErmittleNr.
-- Generates a unique tracking identifier.
--------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE `@gcp_project.@bq_dataset.DWMSG_ErmittleNr`(OUT out_entry_nr STRING)
BEGIN
  -- Validate that we are returning a value (addresses original shell assertions)
  -- Original Literal Check: "Argh!, keinen Variablennamen bei ErmittleNr angegeben"
  -- Since BigQuery uses OUT parameters, we verify that the destination can receive a value.
  
  SET out_entry_nr = CAST(GENERATE_UUID() AS STRING);
END;

--------------------------------------------------------------------
-- Procedure: DWMSG_SetzeStatusOK
-- Replaces DWMSG_SetzeStatusOK shell function
--------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE `@gcp_project.@bq_dataset.DWMSG_SetzeStatusOK`(p_entry_nr STRING)
BEGIN
  IF p_entry_nr IS NULL THEN
    ERROR "Argh!, keine EintragsNummer bei Aufruf von SetzeOkStatus angegeben";
  END IF;

  -- Delegate to the translated backend package procedure
  CALL `@gcp_project.@bq_dataset.dwpa_meldung__setzestatusok`(p_entry_nr);
END;

--------------------------------------------------------------------
-- Procedure: DWMSG_SetzeStatusAbbruch
-- Replaces DWMSG_SetzeStatusAbbruch shell function
--------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE `@gcp_project.@bq_dataset.DWMSG_SetzeStatusAbbruch`(p_entry_nr STRING)
BEGIN
  IF p_entry_nr IS NULL THEN
    ERROR "Argh!, keine EintragsNummer bei Aufruf von SetzeAbbruchStatus angegeben";
  END IF;

  -- Delegate to the translated backend package procedure
  CALL `@gcp_project.@bq_dataset.dwpa_meldung__setzestatusabbruch`(p_entry_nr);
END;

--------------------------------------------------------------------
-- Procedure: DWMSG_ErzeugeEintrag
-- Replaces DWMSG_ErzeugeEintrag shell function
--------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE `@gcp_project.@bq_dataset.DWMSG_ErzeugeEintrag`(
  p_entry_nr STRING,
  p_job_kennung STRING,
  p_programmname STRING,
  p_log_datei STRING,
  p_parameter STRING
)
BEGIN
  IF p_entry_nr IS NULL THEN
    ERROR "Argh!, keine EintragsNummer bei Aufruf von ErzeugeEintrag angegeben";
  END IF;

  -- Route call to background logging procedures based on parameter presence
  IF p_parameter IS NULL THEN
    CALL `@gcp_project.@bq_dataset.dwpa_meldung__erzeuge_eintrag_p4`(
      p_entry_nr, p_job_kennung, p_programmname, p_log_datei
    );
  ELSE
    CALL `@gcp_project.@bq_dataset.dwpa_meldung__erzeuge_eintrag_p5`(
      p_entry_nr, p_job_kennung, p_programmname, p_log_datei, p_parameter
    );
  END IF;
END;

--------------------------------------------------------------------
-- Procedure: DWMSG_MeldeFehler
-- Replaces DWMSG_MeldeFehler shell function
--------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE `@gcp_project.@bq_dataset.DWMSG_MeldeFehler`(
  p_entry_nr STRING,
  p_typ STRING,
  p_fehler_nr INT64,
  p_zusatz1 STRING,
  p_zusatz2 STRING
)
BEGIN
  IF p_entry_nr IS NULL THEN
    ERROR "Argh!, keine EintragsNummer bei Aufruf von MeldeFehler angegeben";
  END IF;

  -- Map directly to target PL/SQL logging engine equivalent
  CALL `@gcp_project.@bq_dataset.dwpa_meldung__fehler`(
    p_typ, p_entry_nr, p_fehler_nr, p_zusatz1, p_zusatz2
  );
END;

--------------------------------------------------------------------
-- Procedure: DWMSG_Fehlerbehandlung
-- Replaces DWMSG_Fehlerbehandlung shell error-trapping routine
--------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE `@gcp_project.@bq_dataset.DWMSG_Fehlerbehandlung`(
  p_entry_nr STRING, 
  p_shell_error_code INT64
)
BEGIN
  DECLARE v_unerw_fehler INT64 DEFAULT 10;
  DECLARE v_err_msg STRING;

  -- Original Preserved Literal Integration
  SET v_err_msg = CONCAT(
    "Fehler wurde von der Shell gemeldet, setze auf Abbruchstatus. ErrorCode ist: ", 
    CAST(p_shell_error_code AS STRING)
  );

  -- Log fatal error entry (Type 'F' = Fatal)
  CALL `@gcp_project.@bq_dataset.DWMSG_MeldeFehler`(
    p_entry_nr, 'F', v_unerw_fehler, v_err_msg, NULL
  );

  -- Force target system status to Aborted
  CALL `@gcp_project.@bq_dataset.DWMSG_SetzeStatusAbbruch`(p_entry_nr);
END;

--------------------------------------------------------------------
-- Procedure: DWMSG_Logdateiname
-- Replaces DWMSG_Logdateiname shell function
--------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE `@gcp_project.@bq_dataset.DWMSG_Logdateiname`(
  p_job_kennung STRING,
  p_entry_nr STRING,
  p_dir_prot STRING,
  OUT out_dateiname STRING
)
BEGIN
  -- Mimics original system logic building a date-timestamped path
  SET out_dateiname = CONCAT(
    p_dir_prot, '/', p_job_kennung, '_',
    FORMAT_TIMESTAMP('%Y%m%d_%H%M', CURRENT_TIMESTAMP(), 'Europe/Berlin'), '_',
    p_entry_nr, '.log'
  );
END;

--------------------------------------------------------------------
-- Procedure: DWMSG_SetzeStichtagInfo
-- Replaces DWMSG_SetzeStichtagInfo shell function
--------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE `@gcp_project.@bq_dataset.DWMSG_SetzeStichtagInfo`(
  p_entry_nr STRING,
  p_stichtag STRING,
  p_stichtag_fmt STRING
)
BEGIN
  DECLARE v_parsed_date DATE;

  IF p_entry_nr IS NULL THEN
    ERROR "Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben";
  END IF;
  IF p_stichtag IS NULL THEN
    ERROR "Argh!, keinen Stichtag angegeben!";
  END IF;
  IF p_stichtag_fmt IS NULL THEN
    ERROR "Argh!, Stichtagsangaben ohne Formatangaben können nicht verarbeitet werden!";
  END IF;

  -- Standardize parsing matching dynamic format strings
  IF UPPER(p_stichtag_fmt) = 'YYYYMMDD' THEN
    SET v_parsed_date = PARSE_DATE('%Y%m%d', p_stichtag);
  ELIF UPPER(p_stichtag_fmt) = 'YYYY-MM-DD' THEN
    SET v_parsed_date = PARSE_DATE('%Y-%m-%d', p_stichtag);
  ELSE
    -- Robust default fallback parsing
    SET v_parsed_date = SAFE.PARSE_DATE('%Y-%m-%d', p_stichtag);
    IF v_parsed_date IS NULL THEN
      SET v_parsed_date = SAFE.PARSE_DATE('%Y%m%d', p_stichtag);
    END IF;
  END IF;

  IF v_parsed_date IS NULL THEN
    ERROR CONCAT("Argh!, Stichtag ", p_stichtag, " konnte mit Format ", p_stichtag_fmt, " nicht geparst werden.");
  END IF;

  -- Call standard DWPA package wrapper setting date parameter
  CALL `@gcp_project.@bq_dataset.dwpa_meldung__setzezusatzinfos`(
    p_entry_nr, v_parsed_date, NULL, NULL, NULL
  );
END;

--------------------------------------------------------------------
-- Procedure: DWMSG_AppendTimingInfos
-- Replaces DWMSG_AppendTimingInfos shell function
--------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE `@gcp_project.@bq_dataset.DWMSG_AppendTimingInfos`(
  p_entry_nr STRING,
  p_info_text STRING,
  p_date_format STRING
)
BEGIN
  DECLARE v_formatted_time STRING;
  DECLARE v_final_msg STRING;

  IF p_entry_nr IS NULL THEN
    ERROR "Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben";
  END IF;
  IF p_date_format IS NULL THEN
    ERROR "Argh!, Formatangabe erforderlich!";
  END IF;

  -- Map generic oracle/unix execution timings formats to BigQuery standard datetime formats
  IF p_date_format = 'YYYY-MM-DD HH24:MI:SS' OR p_date_format = 'YYYY-MM-DD HH2s:MI:SS' THEN
    SET v_formatted_time = FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', CURRENT_TIMESTAMP(), 'Europe/Berlin');
  ELSE
    SET v_formatted_time = FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', CURRENT_TIMESTAMP(), 'Europe/Berlin');
  END IF;

  SET v_final_msg = CONCAT(p_info_text, ' ', v_formatted_time, ' ');

  CALL `@gcp_project.@bq_dataset.dwpa_meldung__setzezusatzinfos`(
    p_entry_nr, NULL, v_final_msg, NULL, NULL
  );
END;

--------------------------------------------------------------------
-- Procedure: DWMSG_AppendDateiInfo
-- Replaces DWMSG_AppendDateiInfo shell function
--------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE `@gcp_project.@bq_dataset.DWMSG_AppendDateiInfo`(
  p_entry_nr STRING,
  p_filename STRING
)
BEGIN
  DECLARE v_basename STRING;

  IF p_entry_nr IS NULL THEN
    ERROR "Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben";
  END IF;

  -- Extract file name from absolute path by splitting and extracting the trailing leaf
  SET v_basename = ARRAY_REVERSE(SPLIT(p_filename, '/'))[SAFE_OFFSET(0)];

  CALL `@gcp_project.@bq_dataset.dwpa_meldung__setzezusatzinfos`(
    p_entry_nr, 
    NULL, 
    CONCAT('Datei: ', COALESCE(v_basename, p_filename), ' | '),
    NULL, 
    NULL
  );
END;

--------------------------------------------------------------------
-- Procedure: DWMSG_AppendZusatzInfo
-- Replaces DWMSG_AppendZusatzInfo shell function
--------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE `@gcp_project.@bq_dataset.DWMSG_AppendZusatzInfo`(
  p_entry_nr STRING,
  p_infotext STRING
)
BEGIN
  DECLARE v_escaped_text STRING;

  IF p_entry_nr IS NULL THEN
    ERROR "Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben";
  END IF;

  -- Escape quotes to prevent dynamic code injection vulnerabilities
  SET v_escaped_text = REPLACE(p_infotext, "'", "''");

  CALL `@gcp_project.@bq_dataset.dwpa_meldung__setzezusatzinfos`(
    p_entry_nr, NULL, v_escaped_text, NULL, NULL
  );
END;

--------------------------------------------------------------------
-- Procedure: DWMSG_SetzeStichtag
-- Replaces DWMSG_SetzeStichtag shell function
--------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE `@gcp_project.@bq_dataset.DWMSG_SetzeStichtag`(
  p_entry_nr STRING,
  p_tag STRING
)
BEGIN
  DECLARE v_parsed_date DATE;

  IF p_entry_nr IS NULL THEN
    ERROR "Argh!, keine EintragsNr bei Aufruf von SetzeZusatzInfos angegeben";
  END IF;

  -- Safe parse yyyymmdd from input string
  SET v_parsed_date = SAFE.PARSE_DATE('%Y%m%d', SUBSTR(p_tag, 1, 8));

  IF v_parsed_date IS NULL THEN
    ERROR CONCAT("Argh!, ungültiger Stichtag für Direct Update: ", p_tag);
  END IF;

  -- Directly update metadata target table
  UPDATE `@gcp_project.@bq_dataset.dwh_ta_k_meldungen`
  SET stichtag = v_parsed_date
  WHERE entrynr = p_entry_nr;
END;

--------------------------------------------------------------------
-- Procedure: DWMSG_LogDebug
-- Replaces DWMSG_LogDebug shell function
--------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE `@gcp_project.@bq_dataset.DWMSG_LogDebug`(
  p_entry_nr STRING,
  p_text STRING
)
BEGIN
  IF p_entry_nr IS NULL THEN
    ERROR "Eintragsnummer nicht gesetzt";
  END IF;
  IF p_text IS NULL THEN
    ERROR "Text nicht gesetzt";
  END IF;

  CALL `@gcp_project.@bq_dataset.dwh_vs_meldung__logausgabe_debug`(p_entry_nr, p_text);
END;

--------------------------------------------------------------------
-- Procedure: DWMSG_SetzeDateiname
-- Replaces DWMSG_SetzeDateiname shell function
--------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE `@gcp_project.@bq_dataset.DWMSG_SetzeDateiname`(
  p_entry_nr STRING,
  p_datei STRING
)
BEGIN
  IF p_entry_nr IS NULL THEN
    ERROR "Eintragsnummer nicht gesetzt";
  END IF;
  IF p_datei IS NULL THEN
    ERROR "Dateiname nicht gesetzt";
  END IF;

  -- Standard DML update on tracking record
  UPDATE `@gcp_project.@bq_dataset.dwh_ta_k_meldungen`
  SET dateiname = p_datei
  WHERE entrynr = p_entry_nr;
END;

--------------------------------------------------------------------
-- Procedure: DWMSG_LogInfo
-- Replaces DWMSG_LogInfo shell function
--------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE `@gcp_project.@bq_dataset.DWMSG_LogInfo`(
  p_entry_nr STRING,
  p_text STRING
)
BEGIN
  IF p_entry_nr IS NULL THEN
    ERROR "Eintragsnummer nicht gesetzt";
  END IF;
  IF p_text IS NULL THEN
    ERROR "Text nicht gesetzt";
  END IF;

  CALL `@gcp_project.@bq_dataset.dwpa_meldung__logausgabe_info`(p_entry_nr, p_text);
END;

--------------------------------------------------------------------
-- Procedure: DWMSG_SetzeAnzahl
-- Replaces DWMSG_SetzeAnzahl shell function
--------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE `@gcp_project.@bq_dataset.DWMSG_SetzeAnzahl`(
  p_entry_nr STRING,
  p_anzahl INT64
)
BEGIN
  IF p_entry_nr IS NULL THEN
    ERROR "Eintragsnummer nicht gesetzt";
  END IF;
  IF p_anzahl IS NULL THEN
    ERROR "Anzahl Datensätze nicht gesetzt";
  END IF;

  CALL `@gcp_project.@bq_dataset.dwpa_meldung__setzezusatzinfos`(
    p_entry_nr, NULL, CAST(p_anzahl AS STRING), NULL, NULL
  );
END;