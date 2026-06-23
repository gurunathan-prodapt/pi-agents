-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_geschaeftspartner.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_geschaeftspartner.ksh

-- Description: This DDL creates a BigQuery table to log job execution metadata, status, parameters, and record counts.
-- It replaces the functionality of temporary files and implicitly referenced job control tables in the legacy system.
CREATE TABLE IF NOT EXISTS `project.dataset.job_log` (
    job_kennung STRING NOT NULL,
    eintrags_nr STRING,
    tab_name STRING,
    stichtag DATE,
    status STRING NOT NULL,
    record_count INT64,
    message STRING,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);