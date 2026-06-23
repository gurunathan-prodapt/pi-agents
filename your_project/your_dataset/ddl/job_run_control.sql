-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_discount.ksh
CREATE TABLE `your_project.your_dataset.job_run_control` (
    job_kennung STRING NOT NULL OPTIONS(description="Unique identifier for the job."),
    eintrags_nr STRING NOT NULL OPTIONS(description="Entry number or instance identifier for the job run."),
    script_name STRING NOT NULL OPTIONS(description="Name of the script or procedure."),
    records_processed INT64 OPTIONS(description="Number of records processed during the job run."),
    update_ts TIMESTAMP NOT NULL OPTIONS(description="Timestamp of the last update to this control record.")
);