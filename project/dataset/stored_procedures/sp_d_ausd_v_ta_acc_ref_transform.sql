-- BigQuery Stored Procedure for Data Transformation
-- Replaces: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_acc_ref.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_acc_ref.ksh

CREATE OR REPLACE PROCEDURE `project.dataset.sp_d_ausd_v_ta_acc_ref_transform`()
BEGIN
    DECLARE v_datum STRING;

    -- Determine the cut-off date
    SET v_datum = (
        SELECT IFNULL(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')
        FROM `project.dataset.dwtk_meldungen` AS m
        WHERE m.job_kennung = 'BERT_DROP_TEMP_TABLE'
    );

    -- Clear the target table
    DELETE FROM `project.dataset.sof_ta_acc_ref` WHERE TRUE;

    -- Insert data into the target table
    INSERT INTO `project.dataset.sof_ta_acc_ref` (
        acc_ref_id,
        account_reference
    )
    SELECT
        ar.acc_ref_id,
        ar.account_reference
    FROM
        `project.dataset.cds_ta_acc_ref` AS ar
    WHERE
        ar.insert_at <= PARSE_DATE('%Y%m%d', v_datum)
        AND (ar.modified_at IS NULL OR ar.modified_at > PARSE_DATE('%Y%m%d', v_datum))
        AND ar.valid_from <= PARSE_DATE('%Y%m%d', v_datum)
        AND (ar.valid_to IS NULL OR ar.valid_to > PARSE_DATE('%Y%m%d', v_datum))
        AND ar.is_production = 1;

END;