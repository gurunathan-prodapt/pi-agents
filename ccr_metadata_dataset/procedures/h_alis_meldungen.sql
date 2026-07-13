-- =====================================================================
-- Target Platform: Google BigQuery
-- Source Job: TEST.NEW_HOUSEKEEPING_JOB
-- Target File: ccr_metadata_dataset/procedures/h_alis_meldungen.sql
-- =====================================================================

-- =====================================================================
-- 0. UNDERLYING LOGGING STORE SCHEMA (MIGRATED FROM ORACLE)
-- =====================================================================

-- Main Execution Entry Log Table
CREATE TABLE IF NOT EXISTS `ccr_metadata_dataset.MELDUNG` (
  eintrags_nr INT64 NOT NULL OPTIONS(description="Unique tracking execution identifier"),
  job_kennung STRING OPTIONS(description="Job execution identity code"),
  programmname STRING OPTIONS(description="Path of execution runtime or script"),
  log_datei STRING OPTIONS(description="Target output system log file path"),
  parameter STRING OPTIONS(description="Input parameters parsed to execution process"),
  stichtag DATE OPTIONS(description="Operational reporting business key"),
  anzahl INT64 OPTIONS(description="Execution metric: processed record count"),
  datei STRING OPTIONS(description="Output target filename tracked"),
  zusatz STRING OPTIONS(description="Operational auxiliary context / payload"),
  status STRING OPTIONS(description="Running status of the job tracking (RUNNING, SUCCESS, ERROR)"),
  created_at TIMESTAMP OPTIONS(description="Internal insert audit timestamp"),
  updated_at TIMESTAMP OPTIONS(description="Internal last modification audit timestamp")
)
CLUSTER BY eintrags_nr;

-- Granular Logging Messages Table (System, Debug, Info, Warnings)
CREATE TABLE IF NOT EXISTS `ccr_metadata_dataset.MELDUNG_LOG` (
  eintrags_nr INT64 NOT NULL,
  log_timestamp TIMESTAMP NOT NULL,
  log_level STRING NOT NULL,
  message STRING NOT NULL
)
CLUSTER BY eintrags_nr;

-- =====================================================================
-- 1. BASE SYSTEM CORE MAPPED PROCEDURES (Simulating Oracle Package API)
-- =====================================================================

-- Generates a unique Tracking Entry ID using BigQuery's native hashing
CREATE OR REPLACE PROCEDURE `ccr_metadata_dataset.Erzeuge_EintragNr`(OUT v_var INT64)
BEGIN
  -- Simulates sequence generation safely across concurrent runs using dynamic random generation
  SET v_var = CAST(FLOOR(100000000 + RAND() * 900000000) AS INT64);
END;

-- Creates the primary entry record
CREATE OR REPLACE PROCEDURE `ccr_metadata_dataset.Erzeuge_Eintrag`(
  v_EintragsNr INT64,
  v_JobKennung STRING,
  v_Programmname STRING,
  v_LogDatei STRING,
  v_Parameter STRING
)
BEGIN
  INSERT INTO `ccr_metadata_dataset.MELDUNG` (
    eintrags_nr, job_kennung, programmname, log_datei, parameter, status, created_at, updated_at
  )
  VALUES (
    v_EintragsNr, v_JobKennung, v_Programmname, v_LogDatei, v_Parameter, 'RUNNING', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
  );
END;

-- Standardizes transactional mutations on auxiliary logging metrics
CREATE OR REPLACE PROCEDURE `ccr_metadata_dataset.SetzeZusatzInfos`(
  v_EintragsNr INT64,
  v_Stichtag DATE,
  v_Anzahl INT64,
  v_Datei STRING,
  v_Zusatz STRING
)
BEGIN
  UPDATE `ccr_metadata_dataset.MELDUNG`
  SET 
    stichtag = COALESCE(v_Stichtag, stichtag),
    anzahl = COALESCE(v_Anzahl, anzahl),
    datei = COALESCE(v_Datei, datei),
    zusatz = COALESCE(v_Zusatz, zusatz),
    updated_at = CURRENT_TIMESTAMP()
  WHERE eintrags_nr = v_EintragsNr;
END;

-- Internal Log Routing Procedures
CREATE OR REPLACE PROCEDURE `ccr_metadata_dataset.LogAusgabe_Debug`(v_EintragsNr INT64, v_Text STRING)
BEGIN
  INSERT INTO `ccr_metadata_dataset.MELDUNG_LOG` (eintrags_nr, log_timestamp, log_level, message)
  VALUES (v_EintragsNr, CURRENT_TIMESTAMP(), 'DEBUG', v_Text);
END;

CREATE OR REPLACE PROCEDURE `ccr_metadata_dataset.LogAusgabe_Info`(v_EintragsNr INT64, v_Text STRING)
BEGIN
  INSERT INTO `ccr_metadata_dataset.MELDUNG_LOG` (eintrags_nr, log_timestamp, log_level, message)
  VALUES (v_EintragsNr, CURRENT_TIMESTAMP(), 'INFO', v_Text);
END;

CREATE OR REPLACE PROCEDURE `ccr_metadata_dataset.LogAusgabe_Warn`(v_EintragsNr INT64, v_Text STRING)
BEGIN
  INSERT INTO `ccr_metadata_dataset.MELDUNG_LOG` (eintrags_nr, log_timestamp, log_level, message)
  VALUES (v_EintragsNr, CURRENT_TIMESTAMP(), 'WARNING', v_Text);
END;

-- Registers failure state
CREATE OR REPLACE PROCEDURE `ccr_metadata_dataset.Fehler`(v_EintragsNr INT64, v_Fehler INT64, v_Text STRING)
BEGIN
  UPDATE `ccr_metadata_dataset.MELDUNG`
  SET status = 'ERROR', updated_at = CURRENT_TIMESTAMP()
  WHERE eintrags_nr = v_EintragsNr;

  INSERT INTO `ccr_metadata_dataset.MELDUNG_LOG` (eintrags_nr, log_timestamp, log_level, message)
  VALUES (v_EintragsNr, CURRENT_TIMESTAMP(), CONCAT('ERROR_CODE_', CAST(v_Fehler AS STRING)), v_Text);
END;

-- Registers execution completion state
CREATE OR REPLACE PROCEDURE `ccr_metadata_dataset.Fertig`(v_EintragsNr INT64)
BEGIN
  UPDATE `ccr_metadata_dataset.MELDUNG`
  SET status = 'SUCCESS', updated_at = CURRENT_TIMESTAMP()
  WHERE eintrags_nr = v_EintragsNr;
END;


-- =====================================================================
-- 2. REUSABLE WRAPPER PROCEDURES (Mapped shell module routines)
-- =====================================================================

-- ---------------------------------------------------------------------
-- Procedure: CCRMSG_ErmittleNr
-- ---------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE `ccr_metadata_dataset.CCRMSG_ErmittleNr`(OUT v_var INT64)
BEGIN
  CALL `ccr_metadata_dataset.Erzeuge_EintragNr`(v_var);
END;

-- ---------------------------------------------------------------------
-- Procedure: CCRMSG_ErzeugeEintrag
-- ---------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE `ccr_metadata_dataset.CCRMSG_ErzeugeEintrag`(
  v_EintragsNr INT64,
  v_JobKennung STRING,
  v_Programmname STRING,
  v_LogDatei STRING,
  v_Parameter STRING
)
BEGIN
  IF v_EintragsNr IS NULL THEN
    ERROR("Keine EintragsNummer bei Aufruf von ErzeugeEintrag angegeben");
  END IF;

  CALL `ccr_metadata_dataset.Erzeuge_Eintrag`(v_EintragsNr, v_JobKennung, v_Programmname, v_LogDatei, v_Parameter);
END;

-- ---------------------------------------------------------------------
-- Procedure: CCRMSG_SetzeStichtag
-- ---------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE `ccr_metadata_dataset.CCRMSG_SetzeStichtag`(
  v_EintragsNr INT64,
  v_Stichtag STRING,
  v_Format STRING
)
BEGIN
  DECLARE parsed_date DATE;
  
  IF v_EintragsNr IS NULL THEN
    ERROR("Eintragsnummer nicht gesetzt");
  END IF;
  IF v_Stichtag IS NULL THEN
    ERROR("Stichtag nicht gesetzt");
  END IF;
  IF v_Format IS NULL THEN
    ERROR("Format fuer den Stichtag nicht gesetzt");
  END IF;

  -- Translates legacy Oracle DATE format patterns to BigQuery Standard SQL FORMAT_DATE conventions
  SET parsed_date = SAFE.PARSE_DATE(
    CASE 
      WHEN UPPER(v_Format) = 'DD.MM.YYYY' THEN '%d.%m.%Y'
      WHEN UPPER(v_Format) = 'YYYY-MM-DD' THEN '%Y-%m-%d'
      WHEN UPPER(v_Format) = 'YYYYMMDD'   THEN '%Y%m%d'
      ELSE v_Format -- Fallback pattern if BQ notation is supplied directly
    END,
    v_Stichtag
  );

  IF parsed_date IS NULL THEN
    ERROR(CONCAT("Invalid date string or unsupported format structure: ", v_Stichtag, " Format: ", v_Format));
  END IF;

  CALL `ccr_metadata_dataset.SetzeZusatzInfos`(v_EintragsNr, parsed_date, NULL, NULL, NULL);
END;

-- ---------------------------------------------------------------------
-- Procedure: CCRMSG_SetzeAnzahl
-- ---------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE `ccr_metadata_dataset.CCRMSG_SetzeAnzahl`(
  v_EintragsNr INT64,
  v_Anzahl INT64
)
BEGIN
  IF v_EintragsNr IS NULL THEN
    ERROR("Eintragsnummer nicht gesetzt");
  END IF;
  IF v_Anzahl IS NULL THEN
    ERROR("Anzahl Datensätze nicht gesetzt");
  END IF;

  CALL `ccr_metadata_dataset.SetzeZusatzInfos`(v_EintragsNr, NULL, v_Anzahl, NULL, NULL);
END;

-- ---------------------------------------------------------------------
-- Procedure: CCRMSG_SetzeDateiname
-- ---------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE `ccr_metadata_dataset.CCRMSG_SetzeDateiname`(
  v_EintragsNr INT64,
  v_Datei STRING
)
BEGIN
  IF v_EintragsNr IS NULL THEN
    ERROR("Eintragsnummer nicht gesetzt");
  END IF;
  IF v_Datei IS NULL THEN
    ERROR("Dateiname nicht gesetzt");
  END IF;

  CALL `ccr_metadata_dataset.SetzeZusatzInfos`(v_EintragsNr, NULL, NULL, v_Datei, NULL);
END;

-- ---------------------------------------------------------------------
-- Procedure: CCRMSG_SetzeZusatzinfos
-- ---------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE `ccr_metadata_dataset.CCRMSG_SetzeZusatzinfos`(
  v_EintragsNr INT64,
  v_Zusatz STRING
)
BEGIN
  IF v_EintragsNr IS NULL THEN
    ERROR("Eintragsnummer nicht gesetzt");
  END IF;
  IF v_Zusatz IS NULL THEN
    ERROR("Zusatzinfos nicht gesetzt");
  END IF;

  CALL `ccr_metadata_dataset.SetzeZusatzInfos`(v_EintragsNr, NULL, NULL, NULL, v_Zusatz);
END;

-- ---------------------------------------------------------------------
-- Procedure: CCRMSG_LogDebug
-- ---------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE `ccr_metadata_dataset.CCRMSG_LogDebug`(
  v_EintragsNr INT64,
  v_Text STRING
)
BEGIN
  IF v_EintragsNr IS NULL THEN
    ERROR("Eintragsnummer nicht gesetzt");
  END IF;
  IF v_Text IS NULL THEN
    ERROR("Text nicht gesetzt");
  END IF;

  CALL `ccr_metadata_dataset.LogAusgabe_Debug`(v_EintragsNr, v_Text);
END;

-- ---------------------------------------------------------------------
-- Procedure: CCRMSG_LogInfo
-- ---------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE `ccr_metadata_dataset.CCRMSG_LogInfo`(
  v_EintragsNr INT64,
  v_Text STRING
)
BEGIN
  IF v_EintragsNr IS NULL THEN
    ERROR("Eintragsnummer nicht gesetzt");
  END IF;
  IF v_Text IS NULL THEN
    ERROR("Text nicht gesetzt");
  END IF;

  CALL `ccr_metadata_dataset.LogAusgabe_Info`(v_EintragsNr, v_Text);
END;

-- ---------------------------------------------------------------------
-- Procedure: CCRMSG_LogWarn
-- ---------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE `ccr_metadata_dataset.CCRMSG_LogWarn`(
  v_EintragsNr INT64,
  v_Text STRING
)
BEGIN
  IF v_EintragsNr IS NULL THEN
    ERROR("Eintragsnummer nicht gesetzt");
  END IF;
  IF v_Text IS NULL THEN
    ERROR("Text nicht gesetzt");
  END IF;

  CALL `ccr_metadata_dataset.LogAusgabe_Warn`(v_EintragsNr, v_Text);
END;

-- ---------------------------------------------------------------------
-- Procedure: CCRMSG_Fehler
-- ---------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE `ccr_metadata_dataset.CCRMSG_Fehler`(
  v_EintragsNr INT64,
  v_Fehler INT64,
  v_Text STRING
)
BEGIN
  IF v_EintragsNr IS NULL THEN
    ERROR("Eintragsnummer nicht gesetzt");
  END IF;
  IF v_Fehler IS NULL THEN
    ERROR("Fehlertyp nicht gesetzt");
  END IF;
  IF v_Text IS NULL THEN
    ERROR("Text nicht gesetzt");
  END IF;

  CALL `ccr_metadata_dataset.Fehler`(v_EintragsNr, v_Fehler, v_Text);
END;

-- ---------------------------------------------------------------------
-- Procedure: CCRMSG_Fertig
-- ---------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE `ccr_metadata_dataset.CCRMSG_Fertig`(
  v_EintragsNr INT64
)
BEGIN
  IF v_EintragsNr IS NULL THEN
    ERROR("Eintragsnummer nicht gesetzt");
  END IF;

  CALL `ccr_metadata_dataset.Fertig`(v_EintragsNr);
END;

-- ---------------------------------------------------------------------
-- Procedure: CCRMSG_Logdateiname
-- ---------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE `ccr_metadata_dataset.CCRMSG_Logdateiname`(
  OUT v_VarName STRING,
  v_EintragsNr INT64,
  v_JobKennung STRING,
  v_CCR_DIR_PROT STRING
)
BEGIN
  DECLARE v_TimestampString STRING;
  
  -- Generates matching shell timestamp formatting: date '+%Y%m%d_%H%M'
  SET v_TimestampString = FORMAT_DATETIME('%Y%m%d_%H%M', CURRENT_DATETIME('Europe/Berlin'));
  
  -- Assembles matching GCS URI or structural log file name string
  SET v_VarName = CONCAT(v_CCR_DIR_PROT, '/', v_JobKennung, '_', v_TimestampString, '_', CAST(v_EintragsNr AS STRING), '.log');
END;

-- ---------------------------------------------------------------------
-- Procedure: CCRMSG_Logdateiname_Parallel
-- ---------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE `ccr_metadata_dataset.CCRMSG_Logdateiname_Parallel`(
  OUT v_VarName STRING,
  v_EintragsNr INT64,
  v_JobKennung STRING,
  v_ParNr INT64,
  v_CCR_DIR_PROT STRING
)
BEGIN
  DECLARE v_TimestampString STRING;
  
  -- Generates matching shell timestamp formatting: date '+%Y%m%d_%H%M'
  SET v_TimestampString = FORMAT_DATETIME('%Y%m%d_%H%M', CURRENT_DATETIME('Europe/Berlin'));
  
  -- Assembles matching GCS URI or structural log file name string for parallel processes
  SET v_VarName = CONCAT(v_CCR_DIR_PROT, '/', v_JobKennung, '_', v_TimestampString, '_', CAST(v_EintragsNr AS STRING), '.', CAST(v_ParNr AS STRING), '.log');
END;