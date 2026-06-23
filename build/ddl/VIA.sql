-- BigQuery DDL for legacy target table VIA (placeholder as it's not written by the migrated SQL)
-- Original job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_barrier_zusgf.ksh
CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.VIA` (
    id INT64,
    data STRING,
    status STRING
);