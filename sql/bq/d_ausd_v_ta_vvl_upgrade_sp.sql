--
-- Target code for legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_vvl_upgrade.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vvl_upgrade.ksh
--
-- This BigQuery stored procedure migrates the logic from the original Oracle SQL script.
-- It prepares data for VVL upgrade by truncating a target table and then inserting transformed data
-- from several source tables, including a lookup for upgrade reasons and a subquery for the latest upgrade date.
--
CREATE OR REPLACE PROCEDURE `your_gcp_project_id.your_bq_dataset_id.d_ausd_v_ta_vvl_upgrade_sp`()
BEGIN
    DECLARE v_datum STRING;

    -- Determine v_datum from dwtk_meldungen
    SET v_datum = (
        SELECT
            IFNULL(FORMAT_DATE('%Y%m%d', DATE(MAX(m.timecreated))), '19000101')
        FROM
            `your_gcp_project_id.isbert_schema.dwtk_meldungen` AS m
        WHERE
            m.job_kennung = 'BERT_DROP_TEMP_TABLE'
    );

    -- Truncate the target table
    TRUNCATE TABLE `your_gcp_project_id.your_bq_dataset_id.sof_ta_vvl_upgrade`;

    -- Insert transformed data into the target table
    INSERT INTO `your_gcp_project_id.your_bq_dataset_id.sof_ta_vvl_upgrade` (
        vertrags_id,
        upgradegrund,
        upgradedatum
    )
    SELECT
        vvl.vertrags_id,
        CASE
            WHEN ba.beschreibung = 'DPPS Diensttyp A13 (EG-Upgrade)' THEN 'Endgeraeteupgrade'
            ELSE ba.beschreibung
        END AS upgradegrund,
        vvl2.upgr_datum AS upgradedatum
    FROM
        `your_gcp_project_id.your_bq_dataset_id.sof_ta_vvl_dwh` AS vvl
    JOIN
        `your_gcp_project_id.your_bq_dataset_id.dwh_ta_l_bindefr_aendgr_carm` AS ba
    ON
        ba.vvl_aendgrund_id = vvl.vvl_aendgrund_id
JOIN
    (
        SELECT
            vertrags_id,
            MAX(aenderung_am) AS upgr_datum
        FROM
            `your_gcp_project_id.your_bq_dataset_id.sof_ta_vvl_dwh` AS vvlt
        GROUP BY
            vertrags_id
    ) AS vvl2
ON
    vvl.vertrags_id = vvl2.vertrags_id
    AND vvl.aenderung_am = vvl2.upgr_datum;

END;