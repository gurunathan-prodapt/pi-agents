-- BigQuery DDL for the job_control table (Optional)
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_instance.ksh
-- This table is to be used if the commented-out job management logic (FOSJobDeaktivate, FOSJobErzeugeEintrag)
-- from the original ksh script needs to be reactivated in BigQuery.
--
-- Please replace `project.dataset` with your actual GCP Project ID and BigQuery Dataset ID.

CREATE TABLE IF NOT EXISTS `project.dataset.job_control` (
    tab_name STRING,
    status STRING,
    mode STRING,
    from_date DATE,
    to_date DATE,
    job_type STRING,
    restart_flag STRING,
    records INT64,
    description STRING
);