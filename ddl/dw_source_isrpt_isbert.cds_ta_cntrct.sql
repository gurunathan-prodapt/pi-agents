-- BigQuery DDL for source table dw_source_isrpt_isbert.cds_ta_cntrct
-- Replaces usage in vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_instance.ksh
-- and vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_bpr_instance.sql

CREATE TABLE IF NOT EXISTS `dw_source_isrpt_isbert.cds_ta_cntrct`
(
    cntrct_id          INT64     NOT NULL OPTIONS(description="Contract ID."),
    cntrct_st          INT64     OPTIONS(description="Contract status."),
    redundant_owner_id INT64     OPTIONS(description="Redundant owner ID."),
    insert_at          DATE      OPTIONS(description="Insertion date."),
    modified_at        DATE      OPTIONS(description="Modification date."),
    valid_from         DATE      OPTIONS(description="Validity start date."),
    valid_to           DATE      OPTIONS(description="Validity end date."),
    is_production      INT64     OPTIONS(description="Production flag (0 or 1)."),
    cntrct_ty          INT64     OPTIONS(description="Contract type."),
    cntrct_parent      INT64     OPTIONS(description="Parent contract ID."),
    -- Additional columns from the source system
    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Record creation timestamp in BigQuery.")
);