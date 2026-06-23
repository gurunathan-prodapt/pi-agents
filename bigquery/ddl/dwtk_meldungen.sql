--
-- DDL for dw_meldung (inferred from d_ausd_v_ta_barrier_zusgf.sql)
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_barrier_zusgf.ksh
--
CREATE TABLE IF NOT EXISTS `project.dataset.dwtk_meldungen` (
    job_kennung STRING NOT NULL,
    timecreated TIMESTAMP,
    -- Add other columns if they exist in the source system, e.g.,
    -- message_text STRING
);