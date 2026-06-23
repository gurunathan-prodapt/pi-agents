-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_beschr.ksh
-- Target BigQuery DDL for source table DWH_TA_C_VERTRAG.

CREATE TABLE IF NOT EXISTS `project.dataset.DWH_TA_C_VERTRAG` (
    DWH_VERTRAG_ID INT64,
    Gueltig_von DATE,
    Gueltig_bis DATE,
    Ladedatum DATE,
    column1 STRING,
    column2 STRING
    -- Add other columns as per actual source schema from DWH$TA_C_VERTRAG
);