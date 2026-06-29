-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_apn.ksh
-- Purpose: Table definition to track successful execution and log operational metrics.

CREATE TABLE IF NOT EXISTS `project.dataset.job_audit_log` (
  tab_name STRING OPTIONS(description="Name of the target table being processed"),
  job_kennung STRING OPTIONS(description="Job identifier from scheduler"),
  eintrags_nr STRING OPTIONS(description="Entry sequence number"),
  stichtag STRING OPTIONS(description="Raw string Stichtag date parameter"),
  records_loaded INT64 OPTIONS(description="Number of records successfully processed for the Stichtag"),
  status STRING OPTIONS(description="Processing status (e.g. SUCCESS, FAILED)"),
  created_at TIMESTAMP OPTIONS(description="Timestamp when the audit log was recorded")
);