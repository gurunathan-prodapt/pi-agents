-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_apn_ve.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_apn_ve.ksh

CREATE TABLE IF NOT EXISTS `my-gcp-project.my_dataset.job_control` (
    job_entry_number INT64 NOT NULL OPTIONS(description="Unique job entry number, equivalent to DW_EintragsNr"),
    job_kennung STRING NOT NULL OPTIONS(description="Identifier for the job, equivalent to JobKennung"),
    script_name STRING NOT NULL OPTIONS(description="Name of the script/procedure being executed"),
    start_time TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the job started"),
    end_time TIMESTAMP OPTIONS(description="Timestamp when the job ended"),
    stichtag DATE OPTIONS(description="Key date for the data processing, equivalent to StichtagInfo"),
    status STRING NOT NULL OPTIONS(description="Current status of the job (e.g., 'RUNNING', 'OK', 'ERROR')"),
    error_code INT64 OPTIONS(description="Error code if the job failed"),
    error_message STRING OPTIONS(description="Error message if the job failed"),
    updated_at TIMESTAMP NOT NULL OPTIONS(description="Timestamp of the last update")
);