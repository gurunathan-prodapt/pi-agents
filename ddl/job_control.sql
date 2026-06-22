-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bcp_msisdn.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bcp_msisdn.ksh

CREATE TABLE IF NOT EXISTS `project.dataset.job_control` (
    job_run_id STRING NOT NULL OPTIONS(description="Unique identifier for a job execution instance"),
    job_name STRING NOT NULL OPTIONS(description="Name of the job, e.g., r_ausd_bp_ta_bcp_msisdn"),
    start_time TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the job execution started"),
    end_time TIMESTAMP OPTIONS(description="Timestamp when the job execution ended"),
    status STRING NOT NULL OPTIONS(description="Current status of the job (RUNNING, SUCCESS, FAILED)"),
    stichtag_param STRING OPTIONS(description="Stichtag parameter passed to the job"),
    wiederanlauf_wert_param INT64 OPTIONS(description="Wiederanlaufwert parameter passed to the job"),
    error_message STRING OPTIONS(description="Detailed error message if the job failed")
);