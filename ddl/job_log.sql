--
-- BigQuery DDL for the job logging table.
-- Replaces: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_discount.ksh
--
CREATE TABLE IF NOT EXISTS `project.dataset.job_log`
(
    job_kennung             STRING NOT NULL,
    eintrags_nr             STRING NOT NULL,
    tab_name                STRING,
    records_processed       INT64,
    status                  STRING NOT NULL, -- e.g., 'STARTED', 'DONE', 'FAILED'
    created_at              TIMESTAMP NOT NULL
);