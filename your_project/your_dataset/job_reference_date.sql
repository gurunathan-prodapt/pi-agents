-- BigQuery table DDL for job_reference_date
-- Replaces 'Stichtag' information handling from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_cntrct_templ.ksh
CREATE TABLE IF NOT EXISTS `your_project.your_dataset.job_reference_date` (
    job_kennung STRING NOT NULL,
    eintrags_nr INT64 NOT NULL,
    referenz_datum DATE NOT NULL,
    gueltig_ab_datum DATE,
    gueltig_bis_datum DATE
);