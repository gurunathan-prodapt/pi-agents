-- BigQuery DDL for job_control table
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_iccid_vertrag.ksh
CREATE TABLE project.dataset.job_control (
    job_entry_nr INT64 NOT NULL OPTIONS(description="Unique job execution identifier"),
    job_name STRING NOT NULL OPTIONS(description="Name of the job being executed"),
    stichtag STRING OPTIONS(description="Key date parameter for the job (DDMMYYYY)"),
    wiederanlaufwert INT64 OPTIONS(description="Restart value parameter for the job"),
    start_timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the job started"),
    end_timestamp TIMESTAMP OPTIONS(description="Timestamp when the job finished"),
    status STRING NOT NULL OPTIONS(description="Current status of the job (e.g., RUNNING, OK, ERROR)"),
    PRIMARY KEY (job_entry_nr) NOT ENFORCED
)
OPTIONS(
    description="Table to track job execution metadata and status"
);