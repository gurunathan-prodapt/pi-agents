--
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_apn_vertrag.sql
-- Job: DW.BERT_AUSD_BP_TA_APN_VERTRAG
--
-- BigQuery Stored Procedure for the core data processing logic.
-- Translates Oracle PL/SQL to BigQuery SQL, replacing procedural loop with STRING_AGG.

CREATE OR REPLACE PROCEDURE `project.sof.sp_d_ausd_bp_ta_apn_vertrag`()
BEGIN
    DECLARE v_max_timecreated_yyyymmdd STRING;

    -- Equivalent to Oracle: SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101') AS s_datum
    SET v_max_timecreated_yyyymmdd = (
        SELECT
            COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')
        FROM
            `project.isbert_schema.dwtk_meldungen` AS m
        WHERE
            m.job_kennung = 'BERT_DROP_TEMP_TABLE'
    );

    -- Equivalent to Oracle: isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_apn_vertrag');
    TRUNCATE TABLE `project.sof.ta_apn_vertrag`;

    -- Equivalent to the procedural FOR loop with string concatenation and INSERT
    INSERT INTO `project.sof.ta_apn_vertrag` (
        cntrct_id,
        apn_list,
        cntrct_ref_list
    )
    SELECT
        cntrct_id,
        SUBSTR(STRING_AGG(DISTINCT access_point_name ORDER BY access_point_name), 1, 100) AS apn_list,
        SUBSTR(STRING_AGG(DISTINCT cntrct_id_ref ORDER BY cntrct_id_ref), 1, 100) AS cntrct_ref_list
    FROM
        `project.sof.ta_bpr_apn`
    GROUP BY
        cntrct_id;

END;