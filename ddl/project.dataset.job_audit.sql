-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_beschr.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_beschr.ksh
-- Description: Table for storing job execution metadata.
CREATE TABLE IF NOT EXISTS project.dataset.job_audit (
    job_id STRING NOT NULL OPTIONS(description="Unique identifier for the job run"),
    job_name STRING NOT NULL OPTIONS(description="Name of the job (e.g., stored procedure name)"),
    start_time TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the job started"),
    end_time TIMESTAMP OPTIONS(description="Timestamp when the job ended"),
    status STRING OPTIONS(description="Overall status of the job (e.g., 'RUNNING', 'OK', 'ERROR')"),
    parameters JSON OPTIONS(description="Input parameters for the job in JSON format"),
    error_message STRING OPTIONS(description="Error message if the job failed"),
    PRIMARY KEY(job_id) NOT ENFORCED
);