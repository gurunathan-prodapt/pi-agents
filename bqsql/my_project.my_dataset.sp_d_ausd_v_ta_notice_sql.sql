-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_notice.sql
-- This procedure encapsulates the core TRUNCATE and INSERT logic for the ta_notice table.
CREATE OR REPLACE PROCEDURE `my_project.my_dataset.sp_d_ausd_v_ta_notice_sql`(
    IN p_job_kennung STRING,
    IN p_entry_nr INT64,
    OUT p_records_inserted INT64
)
BEGIN
    DECLARE v_datum DATE;

    BEGIN
        -- Determine v_datum from dwtk_meldungen, equivalent to Oracle's SELECT NVL(TO_CHAR(MAX(m.timecreated),'YYYYMMDD'),'19000101')
        SET v_datum = (
            SELECT
                COALESCE(MAX(DATE(m.timecreated)), PARSE_DATE('%Y%m%d', '19000101'))
            FROM
                `my_project.my_dataset.dwtk_meldungen` m
            WHERE
                m.job_kennung = 'BERT_DROP_TEMP_TABLE'
        );

        CALL `my_project.my_dataset.sp_dwmsg_logdateiname`(p_job_kennung, p_entry_nr, CONCAT('Determined v_datum: ', FORMAT_DATE('%Y-%m-%d', v_datum)), 'INFO', 'sp_d_ausd_v_ta_notice_sql');

        -- TRUNCATE TABLE sof$ta_notice
        TRUNCATE TABLE `my_project.my_dataset.sof_ta_notice`;
        CALL `my_project.my_dataset.sp_dwmsg_logdateiname`(p_job_kennung, p_entry_nr, 'TRUNCATE TABLE `my_project.my_dataset.sof_ta_notice` executed.', 'INFO', 'sp_d_ausd_v_ta_notice_sql');

        -- INSERT INTO sof$ta_notice from cds$ta_notice
        INSERT INTO `my_project.my_dataset.sof_ta_notice` (
            cntrct_id,
            valid_from,
            valid_to,
            entry_date_of_notice
        )
        SELECT
            n.cntrct_id,
            n.valid_from,
            n.valid_to,
            n.entry_date_of_notice
        FROM
            `my_project.my_dataset.cds_ta_notice` n
        WHERE
                n.insert_at <= v_datum
        AND     (   n.modified_at IS NULL
                 OR n.modified_at > v_datum     )
        AND     (   n.valid_to IS NULL
                 OR n.valid_to > v_datum        )
        AND     n.is_production = 1;

        SET p_records_inserted = @@row_count;
        CALL `my_project.my_dataset.sp_dwmsg_logdateiname`(p_job_kennung, p_entry_nr, CONCAT('Inserted ', p_records_inserted, ' records into `my_project.my_dataset.sof_ta_notice`.'), 'INFO', 'sp_d_ausd_v_ta_notice_sql');

    EXCEPTION WHEN ERROR THEN
        SET p_records_inserted = -1; -- Indicate failure
        CALL `my_project.my_dataset.sp_dwmsg_fehlerbehandlung`(p_job_kennung, p_entry_nr, ERROR_MESSAGE(), 'sp_d_ausd_v_ta_notice_sql', ERROR_CODE());
        RAISE; -- Re-raise the error to propagate
    END;
END;