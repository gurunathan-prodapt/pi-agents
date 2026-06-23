-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_vvl_dwh.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vvl_dwh.ksh
CREATE OR REPLACE PROCEDURE `your_gcp_project_id.your_bq_dataset_id.d_ausd_v_ta_vvl_dwh_proc`(
    OUT processed_record_count INT64
)
BEGIN
    -- This procedure contains the core data transformation logic from the legacy d_ausd_v_ta_vvl_dwh.sql.
    -- It reads from DWTK_MELDUNGEN and DWH_TA_F_VVL_EREIGNISSE, and writes to SOF_TA_VVL_DWH and VIA.

    DECLARE records_inserted_sof INT64;
    DECLARE records_inserted_via INT64;

    -- Placeholder for re-implementation of functions from legacy DWPA_UTIL_SKRIPT Oracle package.
    -- Example: SELECT your_gcp_project_id.your_bq_dataset_id.DWPA_UTIL_SKRIPT_FUNCTION_REIMPL('arg');

    -- Start transaction
    BEGIN TRANSACTION;

    -- Step 1: Process data and insert into SOF_TA_VVL_DWH
    -- This is a placeholder. The actual logic needs to be extracted from d_ausd_v_ta_vvl_dwh.sql
    INSERT INTO `your_gcp_project_id.your_bq_dataset_id.SOF_TA_VVL_DWH` (id, processed_data, processing_timestamp)
    SELECT
        t1.id,
        TO_JSON(STRUCT(t1.data AS data_from_meldungen, t2.event_data AS data_from_ereignisse)), -- Example: combine data
        CURRENT_TIMESTAMP()
    FROM
        `your_gcp_project_id.your_bq_dataset_id.DWTK_MELDUNGEN` t1
    JOIN
        `your_gcp_project_id.your_bq_dataset_id.DWH_TA_F_VVL_EREIGNISSE` t2
    ON
        t1.id = t2.id
    WHERE
        -- Add actual filtering/transformation logic from d_ausd_v_ta_vvl_dwh.sql
        TRUE; -- Placeholder

    SET records_inserted_sof = ROW_COUNT();

    -- Step 2: Process data and insert/update into VIA
    -- This is a placeholder. The actual logic needs to be extracted from d_ausd_v_ta_vvl_dwh.sql
    -- Assuming a simple insert for now, if it's an update, logic will differ.
    INSERT INTO `your_gcp_project_id.your_bq_dataset_id.VIA` (id, status_info, update_timestamp)
    SELECT
        t1.id,
        TO_JSON(STRUCT('Processed' AS status, records_inserted_sof AS record_count)), -- Example status
        CURRENT_TIMESTAMP()
    FROM
        `your_gcp_project_id.your_bq_dataset_id.DWTK_MELDUNGEN` t1
    JOIN
        `your_gcp_project_id.your_bq_dataset_id.DWH_TA_F_VVL_EREIGNISSE` t2
    ON
        t1.id = t2.id
    WHERE
        -- Add actual filtering/transformation logic from d_ausd_v_ta_vvl_dwh.sql
        TRUE; -- Placeholder

    SET records_inserted_via = ROW_COUNT();

    SET processed_record_count = records_inserted_sof + records_inserted_via; -- Sum of records affecting target tables

    COMMIT TRANSACTION;

EXCEPTION WHEN ERROR THEN
    ROLLBACK TRANSACTION;
    RAISE;
END;