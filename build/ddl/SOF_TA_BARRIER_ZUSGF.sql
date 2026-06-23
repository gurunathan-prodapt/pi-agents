-- BigQuery DDL for legacy target table SOF$TA_BARRIER_ZUSGF
-- Original job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_barrier_zusgf.ksh
CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.SOF_TA_BARRIER_ZUSGF` (
    cntrct_id INT64,
    sperrart_alle STRING,
    sperrgrund_alle STRING,
    stilllegungszeitraum_alle STRING,
    sperrgrund_zusgf INT64
);