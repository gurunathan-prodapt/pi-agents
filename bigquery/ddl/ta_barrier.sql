--
-- DDL for ta_barrier (inferred from d_ausd_v_ta_barrier_zusgf.sql)
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_barrier_zusgf.ksh
--
CREATE TABLE IF NOT EXISTS `project.dataset.ta_barrier` (
    cntrct_id INT64 NOT NULL,
    sperrart STRING,
    sperrgrund STRING,
    ist_stillegung INT64, -- 1 for true, 0 for false
    sperr_beginn DATE,
    sperr_ende DATE,
    barrier_reason_cv INT64,
    -- Add other columns if they exist in the source system
    PRIMARY KEY (cntrct_id) NOT ENFORCED
);