-- BigQuery Stored Procedure for data processing
-- Replaces: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/sql/d_ausd_v_ta_bp_ref.sql
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_bp_ref.ksh

CREATE OR REPLACE PROCEDURE `my_project.my_dataset.sp_d_ausd_v_ta_bp_ref`(
  IN v_datum DATE,
  IN p_EintragsNr INT64,
  IN p_JobKennung STRING,
  OUT processed_records INT64
)
BEGIN
  DECLARE record_count INT64 DEFAULT 0;

  -- Error handling block for data processing
  BEGIN
    -- Clear target table
    TRUNCATE TABLE `my_project.my_dataset.sof_ta_bp_ref`;

    -- Load filtered data into sof_ta_bp_ref
    INSERT INTO `my_project.my_dataset.sof_ta_bp_ref` (
      cntrct_cp2_id,
      bp_id
    )
    SELECT
      br.cntrct_cp2_id,
      br.bp_id
    FROM `my_project.my_dataset.cds_ta_bp_ref` AS br
    WHERE DATE(br.insert_at) <= v_datum
      AND (
        br.modified_at IS NULL
        OR DATE(br.modified_at) > v_datum
      )
      AND DATE(br.valid_from) <= v_datum
      AND (
        br.valid_to IS NULL
        OR DATE(br.valid_to) > v_datum
      )
      AND br.is_production = 1
      AND br.bp_ref_ty = 4;

    SET record_count = @@row_count;

    -- MERGE operation on the VIA table (placeholder - details from original not available)
    -- The original SQL script `d_ausd_v_ta_bp_ref.sql` did not contain the MERGE logic for VIA.
    -- This section is a placeholder based on the design document's mention of WRITES_TABLE VIA.
    -- Adjust the JOIN condition and INSERT/UPDATE statements based on the actual schema and logic for the VIA table.
    MERGE INTO `my_project.my_dataset.VIA` AS target
    USING `my_project.my_dataset.sof_ta_bp_ref` AS source
    ON target.via_id = CAST(source.cntrct_cp2_id AS STRING) -- Assuming via_id is derived from cntrct_cp2_id
    WHEN NOT MATCHED THEN
      INSERT (via_id, some_column, updated_at) VALUES (CAST(source.cntrct_cp2_id AS STRING), 'default_val', CURRENT_TIMESTAMP())
    WHEN MATCHED THEN
      UPDATE SET some_column = 'updated_val', updated_at = CURRENT_TIMESTAMP();

    SET processed_records = record_count; -- Return count from the main INSERT

  EXCEPTION WHEN ERROR THEN
    -- Log error to job_error_log and re-raise
    INSERT INTO `my_project.my_dataset.job_error_log` (job_kennung, entry_nr, error_level, error_message)
    VALUES (p_JobKennung, p_EintragsNr, 'ERROR', CONCAT(@@error.message, ' (in sp_d_ausd_v_ta_bp_ref)'));
    RAISE;
  END;
END;