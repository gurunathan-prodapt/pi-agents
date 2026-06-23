-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vertrag_tmp.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vertrag_tmp.ksh

-- Create table to register job metadata.
CREATE TABLE IF NOT EXISTS `my_gcp_project.dw_isrpt_isbert_prod.job_registry`
(
    job_run_id STRING NOT NULL OPTIONS(description="Unique identifier for each job execution instance (DW_EintragsNr)"),
    job_kennung STRING NOT NULL OPTIONS(description="Identifier for the job type (JobKennung)"),
    program_name STRING OPTIONS(description="Name of the program/script executed (ProgName)"),
    program_path STRING OPTIONS(description="Original path of the program executed"),
    start_time TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the job execution started"),
    end_time TIMESTAMP OPTIONS(description="Timestamp when the job execution ended"),
    status STRING OPTIONS(description="Overall status of the job execution (OK/ERR)"),
    error_code INT OPTIONS(description="Error code if the job failed"),
    error_message STRING OPTIONS(description="Error message if the job failed"),
    stichtag_info DATE OPTIONS(description="Key date for which the job was run (v_sysdate)")
)
PARTITION BY
    DATE_TRUNC(start_time, DAY)
CLUSTER BY
    job_kennung
OPTIONS(
    description = "Registry for ETL job executions, storing metadata and overall status."
);