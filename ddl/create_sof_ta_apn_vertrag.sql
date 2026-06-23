-- BigQuery DDL for table: sof_ta_apn_vertrag
-- Legacy Target Table for Aggregated Data
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_apn_vertrag.ksh

CREATE TABLE IF NOT EXISTS `your_gcp_project.your_bigquery_dataset.sof_ta_apn_vertrag`
(
    cntrct_id STRING NOT NULL OPTIONS(description="Contract ID"),
    apn_list STRING OPTIONS(description="Comma-separated list of Access Point Names"),
    contract_ref_list STRING OPTIONS(description="Comma-separated list of Reference Contract IDs"),
    snapshot_date DATE NOT NULL OPTIONS(description="Date for which the data was processed")
)
PARTITION BY snapshot_date
CLUSTER BY cntrct_id;