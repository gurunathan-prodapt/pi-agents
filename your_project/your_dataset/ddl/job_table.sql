-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_discount.ksh
CREATE TABLE `your_project.your_dataset.job_table` (
    job_kennung STRING NOT NULL OPTIONS(description="Unique identifier for the job."),
    eintrags_nr STRING NOT NULL OPTIONS(description="Entry number or instance identifier for the job run."),
    table_name STRING OPTIONS(description="Optional: Name of the table being processed by the job."),
    active_flag BOOL NOT NULL OPTIONS(description="Indicates if the job entry is currently active (TRUE) or inactive (FALSE)."),
    start_ts TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the job execution started."),
    end_ts TIMESTAMP OPTIONS(description="Timestamp when the job execution ended."),
    script_name STRING OPTIONS(description="Name of the script or procedure being executed.")
);