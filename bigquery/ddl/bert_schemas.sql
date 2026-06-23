-- Legacy Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_msisdn.ksh

-- DDL for audit and logging table
CREATE TABLE IF NOT EXISTS dwh_bert_dataset.job_log (
  job_name STRING,
  job_version STRING,
  job_entry_nr STRING, -- Using STRING to accommodate UUID for entry number
  log_level STRING,
  error_code STRING,
  error_argument STRING,
  log_message STRING,
  created_at TIMESTAMP,
  status STRING,
  finished_at TIMESTAMP,
  stichtag DATE,
  wiederanlaufwert STRING
);

-- DDL for target table sof_ta_msisdn
CREATE TABLE IF NOT EXISTS dwh_bert_dataset.sof_ta_msisdn (
  BPR_INSTANCE_ID STRING,
  MSISDN STRING,
  CALLNUMBER_ROLE_ID STRING,
  VALID_TO DATE
);

-- DDL for source table dwtk_meldungen (inferred from usage)
CREATE TABLE IF NOT EXISTS dwh_bert_dataset.dwtk_meldungen (
  timecreated TIMESTAMP,
  job_kennung STRING
);

-- DDL for source table sof_ta_msisdn_his (inferred from usage)
CREATE TABLE IF NOT EXISTS dwh_bert_dataset.sof_ta_msisdn_his (
  bpri_com_id STRING,
  msisdn STRING,
  callnumber_role_id STRING,
  valid_to DATE
);