-- BigQuery DDL for pds$ta_access_point
-- Replaces Oracle table used by vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_apn_carmen.ksh
CREATE TABLE IF NOT EXISTS `your_project.your_dataset.pds$ta_access_point` (
    access_point_id INT64,
    access_point_name STRING,
    insert_at DATE,
    modified_at DATE
);