-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_disc_zusgf.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_disc_zusgf.ksh

CREATE OR REPLACE PROCEDURE `isbert_ds.d_ausd_v_ta_disc_zusgf_sql_logic`()
BEGIN
    DECLARE v_datum STRING;

    -- Determine Stichtag
    SELECT
        COALESCE(FORMAT_DATE('%Y%m%d', MAX(DATE(timecreated))), '19000101')
    INTO v_datum
    FROM
        `isbert_ds.dwtk_meldungen`
    WHERE
        job_kennung = 'BERT_DROP_TEMP_TABLE';

    TRUNCATE TABLE `isbert_ds.sof_ta_disc_zusgf`;

    INSERT INTO `isbert_ds.sof_ta_disc_zusgf`
    (
        cntrct_id,
        cntrct_obj_version,
        disc_vector_ty,
        rabatt_alle
    )
    SELECT
        dzg.cntrct_id,
        dzg.cntrct_obj_version,
        dzg.disc_vector_ty,
        LEFT(con.rabatt_alle, 500)
    FROM
        (
            SELECT DISTINCT
                cntrct_id,
                disc_vector_ty,
                cntrct_obj_version
            FROM
                `isbert_ds.sof_ta_discount`
        ) AS dzg
    LEFT JOIN
        (
            SELECT
                cntrct_id,
                cntrct_obj_version,
                STRING_AGG(CONCAT(rabatt, ' (', CAST(rabatthoehe AS STRING), '%)'), ', ' ORDER BY CONCAT(rabatt, ' (', CAST(rabatthoehe AS STRING), '%)')) AS rabatt_alle
            FROM
                `isbert_ds.sof_ta_discount`
            GROUP BY
                cntrct_id,
                cntrct_obj_version
        ) AS con
    ON
        dzg.cntrct_id = con.cntrct_id
        AND dzg.cntrct_obj_version = con.cntrct_obj_version;
END;