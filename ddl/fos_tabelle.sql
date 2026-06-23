-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_geschaeftspartner.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_geschaeftspartner.ksh

-- Placeholder for FOS-Tabelle. This table is mentioned as the target output
-- in the design document. It is likely a consolidated view or table
-- derived from the intermediate tables populated by d_ausd_geschaeftspartner_bq.sql,
-- such as sof_ta_p_gesch_part, sof_ta_p_dn_nutzer, sof_ta_p_evn_empf.
-- For this migration, we create a simple placeholder table.
CREATE TABLE IF NOT EXISTS isbert_target_ds.FOS_Tabelle (
    contract_id STRING,
    customer_name STRING,
    city STRING,
    segment_id STRING,
    processing_date DATE
);