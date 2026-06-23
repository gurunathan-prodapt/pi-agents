-- BigQuery DDL for sof_ta_bpr_instance_staging table
-- This table acts as a staging area, similar to the `sof$ta_bpr_instance` used in the legacy SQL script.
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_instance.ksh

CREATE TABLE IF NOT EXISTS my_gcp_project.my_bq_dataset.sof_ta_bpr_instance_staging
(
    CNTRCT_ID       INT64       NOT NULL,
    BPR_ID          INT64       NOT NULL,
    BPR_INSTANCE_ID INT64       NOT NULL,
    ICCID           STRING,
    IMSI_MCC        INT64,
    IMSI_MNC        INT64,
    IMSI_HLR        INT64,
    IMSI_SI         INT64,
    CNTRCT_ID_REF   INT64,
    processing_date DATE        NOT NULL
)
PARTITION BY processing_date;