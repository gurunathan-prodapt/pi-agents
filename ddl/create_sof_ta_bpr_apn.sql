-- BigQuery DDL for table: sof_ta_bpr_apn
-- Legacy Source: sof$ta_bpr_apn (Oracle)
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_apn_vertrag.ksh

CREATE TABLE IF NOT EXISTS `your_gcp_project.your_bigquery_dataset.sof_ta_bpr_apn`
(
    cntrct_id STRING NOT NULL OPTIONS(description="Contract ID"),
    access_point_name STRING OPTIONS(description="Access Point Name"),
    cntrct_id_ref STRING OPTIONS(description="Reference Contract ID")
);