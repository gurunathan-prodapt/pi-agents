-- Target for: Logging and Status Tables (DDL)
-- Legacy Source: N/A (new BigQuery component)
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_assign.ksh

CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.job_table` (
  job_id STRING NOT NULL OPTIONS(description="Unique identifier for the job definition (e.g., 'R_AUSD_V_TA_INV_ASSIGN')"),
  job_name STRING OPTIONS(description="Descriptive name of the job"),
  entry_nr INT64 NOT NULL OPTIONS(description="Unique entry number for each job execution"),
  status STRING NOT NULL OPTIONS(description="Current status of the job (e.g., 'RUNNING', 'SUCCESS', 'FAILED')"),
  start_time TIMESTAMP OPTIONS(description="Start timestamp of the job execution"),
  end_time TIMESTAMP OPTIONS(description="End timestamp of the job execution"),
  reporting_date DATE OPTIONS(description="The 'stichtag' or reporting date for the job run"),
  processed_rows INT64 OPTIONS(description="Number of rows processed by the job")
);