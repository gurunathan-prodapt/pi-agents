-- DDL for PoolBasisprodukt table
-- Legacy Source: Populated by d_ausd_bp_ta_bpr_basis_his.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_basis_his.ksh

CREATE OR REPLACE TABLE `project.dataset.PoolBasisprodukt`
(
    -- Placeholder columns - actual schema needs to be derived from d_ausd_bp_ta_bpr_basis_his.sql
    id INT64,
    produkt_name STRING,
    stichtag DATE,
    eintrags_nr STRING,
    job_kennung STRING,
    last_update_ts TIMESTAMP
);