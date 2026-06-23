--
-- BigQuery DDL for the target data table `ta_p_discount`.
-- Replaces: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_discount.ksh
--
-- NOTE: The exact schema for ta_p_discount was not provided in the design document.
-- This DDL is a placeholder and should be completed based on the detailed analysis
-- of the source SQL script d_ausd_v_ta_p_discount.sql.
--
CREATE TABLE IF NOT EXISTS `project.dataset.ta_p_discount`
(
    -- Example columns - replace with actual schema from source d_ausd_v_ta_p_discount.sql
    id                      STRING,
    eintrags_nr             STRING,
    job_kennung             STRING,
    discount_value          NUMERIC,
    effective_date          DATE,
    end_date                DATE,
    created_at              TIMESTAMP
);