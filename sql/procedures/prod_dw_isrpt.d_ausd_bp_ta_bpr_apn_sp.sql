-- BigQuery Stored Procedure for the core SQL logic
-- Replaces vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_bp_ta_bpr_apn.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_apn.ksh

CREATE OR REPLACE PROCEDURE prod_dw_isrpt.d_ausd_bp_ta_bpr_apn_sp(
    p_EintragsNr STRING,
    p_JobKennung STRING,
    p_Stichtag_YYYYMMDD STRING, -- Stichtag converted to YYYYMMDD format
    v_restart INT64,
    v_datum_heute_YYYYMMDD STRING,
    v_datum_gestern_YYYYMMDD STRING
)
BEGIN
    -- This procedure simulates the logic from the original d_ausd_bp_ta_bpr_apn.sql
    -- It assumes intermediate source tables exist in prod_dw_isrpt with similar data.
    -- The output of this procedure will directly populate prod_dw_isrpt.PoolBasisprodukt.

    -- DECLARE variable for s_datum (originally from dwtk_meldungen)
    DECLARE v_s_datum STRING;

    -- Simulate fetching s_datum. Replace 'your_project.your_dataset.dwtk_meldungen' with actual source.
    -- For demonstration, setting a default or using a dummy source.
    -- In a real scenario, this would query a migrated version of isbert_schema.dwtk_meldungen
    SET v_s_datum = (
        SELECT
            IFNULL(FORMAT_DATE('%Y%m%d', MAX(m.timecreated)), '19000101')
        FROM
            -- Placeholder: Replace with actual source table for DWTK_MELDUNGEN
            -- Assuming a table like 'prod_dw_source.dwtk_meldungen' exists
            `prod_dw_source.dwtk_meldungen` m -- Example placeholder
        WHERE
            m.job_kennung = 'BERT_DROP_TEMP_TABLE'
    );

    -- step01: lschen der aktuellen tabellen fr den fall eines restarts am gleichen tag
    -- Truncate the target table before inserting new data
    TRUNCATE TABLE prod_dw_isrpt.PoolBasisprodukt;

    -- step10: erstellung einer lokalen apn-vertrge mit allen apn-s und vertragsreferenzen pro vertrag
    -- This INSERT statement directly populates prod_dw_isrpt.PoolBasisprodukt
    -- It assumes sof$ta_bpr_instance and sof$ta_apn_carmen are migrated to BigQuery
    -- (e.g., prod_dw_source.sof_ta_bpr_instance, prod_dw_source.sof_ta_apn_carmen)
    INSERT INTO prod_dw_isrpt.PoolBasisprodukt
    (
        CNTRCT_ID,
        BPR_ID,
        CNTRCT_ID_REF,
        ACCESS_POINT_NAME
    )
    SELECT
        CAST(bp.cntrct_id AS STRING), -- Assuming CNTRCT_ID might be numeric in source, cast to STRING for target DDL
        bp.bpr_id,
        CAST(bp.cntrct_id_ref AS STRING), -- Assuming CNTRCT_ID_REF might be numeric in source, cast to STRING for target DDL
        ap.access_point_name
    FROM
        -- Placeholder: Replace with actual source tables
        `prod_dw_source.sof_ta_bpr_instance` bp,
        `prod_dw_source.sof_ta_apn_carmen` ap
    WHERE
        bp.bpr_id IN (2828, 2829, 2830, 2831, 2925, 2926, 2998, 2999, 3000)
        AND bp.cntrct_id_ref = ap.cntrct_id
    GROUP BY 1, 2, 3, 4; -- GROUP BY to simulate DISTINCT for BigQuery

    -- No explicit COMMIT required in BigQuery DML, it's atomic.

    -- No explicit EXIT SUCCESS, procedure finishes on its own.
END;