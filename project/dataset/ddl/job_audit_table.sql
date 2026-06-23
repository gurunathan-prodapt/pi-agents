-- DDL for project.dataset.job_audit_table
-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_beschr.ksh
--
-- This table serves as a replacement for the legacy job management system (e.g., FOSJobErzeugeEintrag)
-- suggested in the original KornShell script. It logs the status and details of job runs.
--
CREATE TABLE IF NOT EXISTS `project.dataset.job_audit_table`
(
    tab_name STRING OPTIONS(description="Name of the table or process processed"),
    job_status STRING OPTIONS(description="Status of the job ('A' for active/success, 'E' for error)"),
    load_type STRING OPTIONS(description="Type of load (e.g., 'I' for initial, 'U' for update)"),
    stichtag STRING OPTIONS(description="Reference date of the data in DDMMYYYY format as received"),
    run_date DATE OPTIONS(description="Parsed date when the job was logically run (from stichtag)"),
    job_kind STRING OPTIONS(description="Kind of job (e.g., 'J')"),
    restart_flag STRING OPTIONS(description="Flag indicating if it was a restart ('Y' or 'N')"),
    record_count INT64 OPTIONS(description="Number of records processed or loaded"),
    message STRING OPTIONS(description="Additional message or description of the job run, including error messages"),
    insert_timestamp TIMESTAMP OPTIONS(description="Timestamp when the audit record was inserted")
);