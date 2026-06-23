-- BigQuery DDL for source table dw_source_isrpt_isbert.pds_ta_bpri_com
-- Replaces usage in vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_instance.ksh
-- and vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_bpr_instance.sql

CREATE TABLE IF NOT EXISTS `dw_source_isrpt_isbert.pds_ta_bpri_com`
(
    cntrct_id       INT64     NOT NULL OPTIONS(description="Contract ID."),
    bpr_id          INT64     OPTIONS(description="Base Product ID."),
    bpri_com_id     INT64     OPTIONS(description="Base Product Instance Component ID."),
    iccid_mi        INT64     OPTIONS(description="ICCID Major Industry Identifier part."),
    iccid_ii        INT64     OPTIONS(description="ICCID Issuer Identification Number part."),
    iccid_iai       INT64     OPTIONS(description="ICCID Individual Account Identification part."),
    iccid_nr        INT64     OPTIONS(description="ICCID Serial Number part."),
    iccid_cd        INT64     OPTIONS(description="ICCID Check Digit part."),
    imsi_mcc        STRING    OPTIONS(description="IMSI Mobile Country Code."),
    imsi_mnc        STRING    OPTIONS(description="IMSI Mobile Network Code."),
    imsi_hlr        STRING    OPTIONS(description="IMSI Home Location Register part."),
    imsi_si         STRING    OPTIONS(description="IMSI Subscriber Identification part."),
    cntrct_id_ref   INT64     OPTIONS(description="Referenced Contract ID."),
    insert_at       DATE      OPTIONS(description="Insertion date."),
    modified_at     DATE      OPTIONS(description="Modification date."),
    valid_from      DATE      OPTIONS(description="Validity start date."),
    valid_to        DATE      OPTIONS(description="Validity end date."),
    is_production   INT64     OPTIONS(description="Production flag (0 or 1)."),
    -- Additional columns from the source system
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP() OPTIONS(description="Record creation timestamp in BigQuery.")
);