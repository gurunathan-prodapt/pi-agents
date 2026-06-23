--
-- DDL for target_result_table (ta_barrier_zusgf)
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_barrier_zusgf.ksh
--
CREATE TABLE IF NOT EXISTS `project.dataset.ta_barrier_zusgf` (
    cntrct_id INT64 NOT NULL,
    sperrart_alle STRING,
    sperrgrund_alle STRING,
    stilllegungszeitraum_alle STRING,
    sperrgrund_zusgf INT64,
    PRIMARY KEY (cntrct_id) NOT ENFORCED
);