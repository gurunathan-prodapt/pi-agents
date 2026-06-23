--
-- BigQuery DDL for the job_control_table
-- Replaces job state management aspects of vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_c_bfc.ksh
--

CREATE TABLE IF NOT EXISTS `your_project.your_dataset.job_control_table` (
    job_kenn_ung STRING NOT NULL OPTIONS(description="Job identifier from the legacy system (p_JobKennung)"),
    eintrags_nr STRING NOT NULL OPTIONS(description="Entry number from the legacy system (p_EintragsNr)"),
    run_id STRING NOT NULL OPTIONS(description="Unique identifier for the current active run (foreign key to job_run_log)"),
    status STRING NOT NULL OPTIONS(description="Current status of the job (e.g., 'ACTIVE', 'INACTIVE', 'COMPLETED', 'FAILED')"),
    start_timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the job became active"),
    end_timestamp TIMESTAMP OPTIONS(description="Timestamp when the job completed or failed"),
    last_update_timestamp TIMESTAMP NOT NULL OPTIONS(description="Last time the job status was updated")
)
OPTIONS(
    description="Manages the state and concurrency of jobs"
);