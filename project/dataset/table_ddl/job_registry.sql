-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_templ.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_templ.ksh
CREATE TABLE project.dataset.job_registry
(
    job_nr               INT64 NOT NULL OPTIONS(description="Unique job run number"),
    job_kennung          STRING OPTIONS(description="Identifier for the job type (e.g., BERT_V_TA_CNTRCT_TEMPL)"),
    script_name          STRING OPTIONS(description="Name of the script/procedure executed"),
    status               STRING OPTIONS(description="Current status of the job (e.g., RUNNING, SUCCESS, ERROR)"),
    start_timestamp      TIMESTAMP OPTIONS(description="Timestamp when the job started"),
    end_timestamp        TIMESTAMP OPTIONS(description="Timestamp when the job ended"),
    last_update_timestamp TIMESTAMP OPTIONS(description="Last update timestamp for job status")
)
OPTIONS(
    description="Stores job metadata, status, and control information."
);