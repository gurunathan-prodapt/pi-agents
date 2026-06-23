-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_discount.ksh
-- Description: DDL for the target table `ta_p_discount`, based on the output of d_ausd_v_ta_p_discount.sql.
CREATE TABLE IF NOT EXISTS dataset.ta_p_discount (
    cntrct_id STRING,
    disc_vector_ty STRING,
    cntrct_obj_version INT64,
    rabatt_alle NUMERIC,
    contract_number STRING
);