-- File: stored_procedures/r_ausd_bp_ta_msisdn.sql
-- BigQuery Standard SQL
-- Modular, reusable stored procedure translation of k_ausd_bp_ta_msisdn.ksh

CREATE SCHEMA IF NOT EXISTS `gcp-project-placeholder.dw_isbert_dataset`;

CREATE TABLE IF NOT EXISTS `gcp-project-placeholder.dw_isbert_dataset.error_log` (
  job_name STRING,
  error_nr INT64,
  error_arg STRING,
  error_text STRING,
  created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `gcp-project-placeholder.dw_isbert_dataset.job_table` (
  tab_name STRING,
  status_a STRING,
  status_i STRING,
  start_date DATE,
  end_date DATE,
  job_type STRING,
  restart_flag STRING,
  record_count INT64,
  description STRING,
  created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS `gcp-project-placeholder.dw_isbert_dataset.process_log` (
  job_name STRING,
  job_kennung STRING,
  eintrags_nr STRING,
  stichtag DATE,
  records INT64,
  run_date TIMESTAMP,
  restart_value STRING
);

CREATE OR REPLACE PROCEDURE `gcp-project-placeholder.dw_isbert_dataset.r_ausd_bp_ta_msisdn`(
  IN p_JobKennung STRING,
  IN p_EintragsNr STRING,
  IN p_Stichtag STRING,
  IN p_wiederanlaufWert STRING
)
BEGIN
  DECLARE v_job_name STRING DEFAULT 'r_ausd_bp_ta_msisdn';
  DECLARE v_tab_name STRING DEFAULT 'PoolBasisprodukt';
  DECLARE v_restart_value STRING DEFAULT IFNULL(NULLIF(p_wiederanlaufWert, ''), '0');
  DECLARE v_stichtag_date DATE;
  DECLARE v_today DATE DEFAULT CURRENT_DATE();
  DECLARE v_yesterday DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
  DECLARE v_records INT64 DEFAULT 0;
  DECLARE v_err_nr INT64 DEFAULT 0;
  DECLARE v_err_arg STRING DEFAULT '';
  DECLARE v_err_text STRING DEFAULT '';

  -- Reusable validation helpers
  CALL `gcp-project-placeholder.dw_isbert_dataset.sp_validate_required_param`('Jobkennung', p_JobKennung, v_err_nr, v_err_arg, v_err_text);
  IF v_err_nr <> 0 THEN
    CALL `gcp-project-placeholder.dw_isbert_dataset.sp_raise_and_log_error`(v_job_name, v_err_nr, v_err_arg, v_err_text);
  END IF;

  CALL `gcp-project-placeholder.dw_isbert_dataset.sp_validate_required_param`('EintragsNr', p_EintragsNr, v_err_nr, v_err_arg, v_err_text);
  IF v_err_nr <> 0 THEN
    CALL `gcp-project-placeholder.dw_isbert_dataset.sp_raise_and_log_error`(v_job_name, v_err_nr, v_err_arg, v_err_text);
  END IF;

  CALL `gcp-project-placeholder.dw_isbert_dataset.sp_validate_required_param`('Stichtag', p_Stichtag, v_err_nr, v_err_arg, v_err_text);
  IF v_err_nr <> 0 THEN
    CALL `gcp-project-placeholder.dw_isbert_dataset.sp_raise_and_log_error`(v_job_name, v_err_nr, v_err_arg, v_err_text);
  END IF;

  SET v_stichtag_date = SAFE.PARSE_DATE('%d%m%Y', p_Stichtag);
  IF v_stichtag_date IS NULL THEN
    CALL `gcp-project-placeholder.dw_isbert_dataset.sp_raise_and_log_error`(
      v_job_name,
      2,
      p_Stichtag,
      'Ungueltiges Datum'
    );
  END IF;

  -- Core orchestration variables equivalent to legacy helper outputs
  CALL `gcp-project-placeholder.dw_isbert_dataset.sp_execute_core_sql`(
    v_job_name,
    v_tab_name,
    v_stichtag_date,
    v_today,
    v_yesterday,
    v_restart_value,
    v_records
  );

  -- Job table entry equivalent to legacy FOSJobErzeugeEintrag
  INSERT INTO `gcp-project-placeholder.dw_isbert_dataset.job_table`
  (
    tab_name,
    status_a,
    status_i,
    start_date,
    end_date,
    job_type,
    restart_flag,
    record_count,
    description,
    created_at
  )
  VALUES
  (
    v_tab_name,
    'A',
    'I',
    v_stichtag_date,
    v_stichtag_date,
    'J',
    'N',
    v_records,
    'Initialbefuellung',
    CURRENT_TIMESTAMP()
  );

  INSERT INTO `gcp-project-placeholder.dw_isbert_dataset.process_log`
  (
    job_name,
    job_kennung,
    eintrags_nr,
    stichtag,
    records,
    run_date,
    restart_value
  )
  VALUES
  (
    v_job_name,
    p_JobKennung,
    p_EintragsNr,
    v_stichtag_date,
    v_records,
    CURRENT_TIMESTAMP(),
    v_restart_value
  );
END;