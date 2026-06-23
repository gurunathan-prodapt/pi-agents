-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_cntrct_valid.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_valid.ksh
--
-- Migrated core SQL logic from d_ausd_v_ta_cntrct_valid.sql to a BigQuery Stored Procedure.
-- This procedure performs the data transformation and returns the number of inserted rows.
CREATE OR REPLACE PROCEDURE `my_project.my_dataset.d_ausd_v_ta_cntrct_valid`(
    IN p_entry_number STRING,
    OUT p_records_processed INT64
)
BEGIN
    DECLARE v_datum_str STRING;

    -- Determine the date from dwtk_meldungen, equivalent to Oracle's SELECT NVL(TO_CHAR(MAX(m.timecreated),...), '19000101')
    SET v_datum_str = (
        SELECT
            COALESCE(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')
        FROM
            `my_project.my_dataset.dwtk_meldungen` AS m
        WHERE
            m.job_kennung = 'BERT_DROP_TEMP_TABLE'
    );

    -- Equivalent to Oracle's TRUNCATE TABLE sof$ta_cntrct_valid using DWPA_UTIL_SKRIPT
    TRUNCATE TABLE `my_project.my_dataset.sof_ta_cntrct_valid`;

    -- Insert data into sof_ta_cntrct_valid
    INSERT INTO `my_project.my_dataset.sof_ta_cntrct_valid`(
        cntrct_validity_id,
        first_period_id,
        following_period_id,
        first_notice_period_id,
        follow_notice_period_id,
        bfc_age
    )
    SELECT
        cv.cntrct_validity_id,
        cv.first_period_id,
        cv.following_period_id,
        cv.first_notice_period_id,
        cv.follow_notice_period_id,
        cv.insert_at -- This mapping is inferred from "cv.insert_at bfc_age" in original SQL
    FROM
        `my_project.my_dataset.cds_ta_cntrct_validity` AS cv
    WHERE
        cv.insert_at <= PARSE_DATE('%Y%m%d', v_datum_str)
        AND (cv.modified_at IS NULL OR cv.modified_at > PARSE_DATE('%Y%m%d', v_datum_str));

    -- Set the output parameter with the number of affected rows
    SET p_records_processed = @@row_count;

END;