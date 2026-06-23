-- BigQuery DDL for target table dw_source_isrpt_isbert.sof_ta_bpr_instance
-- Populated by vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_bpr_instance.sql
-- Orchestrated by vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_instance.ksh

CREATE TABLE IF NOT EXISTS `dw_source_isrpt_isbert.sof_ta_bpr_instance`
(
    CNTRCT_ID       INT64   NOT NULL OPTIONS(description="Contract ID."),
    BPR_ID          INT64   NOT NULL OPTIONS(description="Base Product ID."),
    BPR_INSTANCE_ID INT64   NOT NULL OPTIONS(description="Base Product Instance ID."),
    ICCID           STRING          OPTIONS(description="Integrated Circuit Card Identifier."),
    IMSI_MCC        STRING          OPTIONS(description="International Mobile Subscriber Identity - Mobile Country Code."),
    IMSI_MNC        STRING          OPTIONS(description="International Mobile Subscriber Identity - Mobile Network Code."),
    IMSI_HLR        STRING          OPTIONS(description="International Mobile Subscriber Identity - Home Location Register."),
    IMSI_SI         STRING          OPTIONS(description="International Mobile Subscriber Identity - Subscriber Identification."),
    CNTRCT_ID_REF   INT64           OPTIONS(description="Referenced Contract ID."),
    processing_date DATE    NOT NULL OPTIONS(description="The key date for which the data was processed.")
)
PARTITION BY processing_date;