-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vertrag_tmp.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vertrag_tmp.ksh

-- Create a dedicated BigQuery dataset for the migrated tables and stored procedures.
CREATE SCHEMA IF NOT EXISTS `my_gcp_project.dw_isrpt_isbert_prod`
OPTIONS(
    description = "Dataset for isrpt.isbert migrations, including contract data reconciliation."
);