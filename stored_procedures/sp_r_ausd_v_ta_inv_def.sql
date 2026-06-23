-- BigQuery Script: Vertragsdatenabgleich wrapper (sp_r_ausd_v_ta_inv_def)
-- Legacy Source: r_ausd_v_ta_inv_def.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_def.ksh

CREATE OR REPLACE PROCEDURE `project_id.dataset_id.sp_r_ausd_v_ta_inv_def`(
  p_h BOOL, -- Equivalent to -h flag, using BOOL for presence
  p_s STRING, -- Equivalent to -s flag
  p_l STRING  -- Equivalent to -l flag
)
BEGIN

  DECLARE ProgName STRING DEFAULT 'Vertragsdatenabgleich';
  DECLARE ProgVersion STRING DEFAULT 'V1.0.0';
  DECLARE ErrNr INT64 DEFAULT 0;
  DECLARE ErrArg STRING DEFAULT '';
  DECLARE DW_EintragsNr INT64;
  DECLARE JobKennung STRING DEFAULT 'BERT_V_TA_INV_DEF';
  DECLARE v_sysdate STRING DEFAULT FORMAT_DATE('%d%m%Y', CURRENT_DATE());
  DECLARE LogDatei STRING;

  -- Usage/help equivalent
  IF p_h THEN
    SELECT
      ProgName AS Programm,
      ProgVersion AS Version,
      'Rahmenskript fuer den Abgleich der Vertragsdaten: Tabelle ta_inv_def.' AS Beschreibung;
    RETURN; -- Exit procedure
  END IF;

  -- Parameter validation (simplified; actual validation depends on p_s, p_l usage)
  -- For example, if p_s or p_l are mandatory and missing:
  -- IF p_s IS NULL THEN SET ErrNr = 193; SET ErrArg = 's'; END IF;
  -- Currently no specific validation rules were provided for p_s and p_l,
  -- so ErrNr remains 0 unless specific logic is added here.

  IF ErrNr <> 0 THEN
    -- CALL to migrated DWMSG_MeldeFehler procedure
    CALL `project_id.dataset_id.sp_dwmsg_meldefehler`(DW_EintragsNr, JobKennung, 'E', ErrNr, ErrArg);
    RAISE USING MESSAGE = CONCAT('Parameterfehler: ', CAST(ErrNr AS STRING), ' ', ErrArg);
  END IF;

  -- Job metadata and logging setup
  CALL `project_id.dataset_id.sp_dwmsg_ermittle_nr`(DW_EintragsNr); -- Returns DW_EintragsNr
  CALL `project_id.dataset_id.sp_dwmsg_logdateiname`(LogDatei, JobKennung, DW_EintragsNr); -- Returns LogDatei
  CALL `project_id.dataset_id.sp_dwmsg_erzeuge_eintrag`(DW_EintragsNr, JobKennung, 'r_ausd_v_ta_inv_def.ksh', LogDatei);
  CALL `project_id.dataset_id.sp_dwmsg_setze_stichtag_info`(DW_EintragsNr, JobKennung, v_sysdate, 'DDMMYYYY');

  BEGIN
    -- Job banner equivalent (log to audit table)
    CALL `project_id.dataset_id.sp_dwmsg_log_info`(DW_EintragsNr, JobKennung, '----------------- Job -----------------------');
    CALL `project_id.dataset_id.sp_dwmsg_log_info`(DW_EintragsNr, JobKennung, CONCAT(' Job-Nr    : \'', CAST(DW_EintragsNr AS STRING), '\''));
    CALL `project_id.dataset_id.sp_dwmsg_log_info`(DW_EintragsNr, JobKennung, CONCAT(' JobKennung: \'', JobKennung, '\''));
    CALL `project_id.dataset_id.sp_dwmsg_log_info`(DW_EintragsNr, JobKennung, CONCAT(' Logdatei  : \'', LogDatei, '\''));
    CALL `project_id.dataset_id.sp_dwmsg_log_info`(DW_EintragsNr, JobKennung, '---------------------------------------------');

    -- Core script invocation equivalent
    -- Pass parameters (like JobKennung, DW_EintragsNr) to the core procedure
    CALL `project_id.dataset_id.sp_k_ausd_v_ta_inv_def`(JobKennung, DW_EintragsNr);

    -- Success handling
    CALL `project_id.dataset_id.sp_dwmsg_log_info`(DW_EintragsNr, JobKennung, 'Die Abarbeitung wurde ohne erkennbare Fehler beendet');
    CALL `project_id.dataset_id.sp_dwmsg_setze_status_ok`(DW_EintragsNr, JobKennung);

  EXCEPTION WHEN ERROR THEN
    CALL `project_id.dataset_id.sp_dwmsg_fehlerbehandlung`(DW_EintragsNr, JobKennung, @@error.message);
    CALL `project_id.dataset_id.sp_dwmsg_log_info`(DW_EintragsNr, JobKennung, 'AppError: Abbruch');
    RAISE; -- Re-raise the error to propagate
  END;

END;