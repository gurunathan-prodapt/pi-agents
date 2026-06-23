-- Migrated from d_ausd_bp_ta_bpr_basis_his.sql
-- Original job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_basis_his.ksh
--
-- BigQuery Stored Procedure: d_ausd_bp_ta_bpr_basis_his
-- Purpose:
--   Rebuild sof.ta_bpr_basis_his with the latest valid basis product instances
--   for active/ended reactivatable contracts as of a given processing date.
--
-- Notes:
--   - BigQuery does not support Oracle-style SQL*Plus scripting, hints, or
--     WHENEVER SQLERROR / TRY...CATCH blocks directly.
--   - Error handling is implemented via BigQuery scripting with EXCEPTION blocks.
--   - This procedure truncates the target table and reloads it.
--   - Source tables `cds.ta_cntrct` and `pds.ta_bpri_com` are assumed to exist in BigQuery
--     with equivalent columns and appropriate data types.
--   - The `&v_carmen` schema prefix has been replaced with explicit dataset names (`cds`, `pds`).
--   - `NVL` converted to `IFNULL`. `TO_DATE` converted to `PARSE_DATE` or direct date comparison.
--   - String concatenation `||` converted to `CONCAT`.
--   - Oracle `/*+ ... */` hints are removed as they are not standard BigQuery syntax.
--   - The `v_datum` logic from `isbert_schema.dwtk_meldungen` is simplified to use the `p_process_date` directly,
--     as per design implication.

CREATE OR REPLACE PROCEDURE `your-gcp-project.sof.d_ausd_bp_ta_bpr_basis_his`(
  IN p_process_date DATE
)
BEGIN
  DECLARE v_process_date DATE DEFAULT IFNULL(p_process_date, CURRENT_DATE());
  DECLARE v_rows_inserted INT64 DEFAULT 0;

  BEGIN
    -- Step 1: Clear target table for restartability
    TRUNCATE TABLE `your-gcp-project.sof.ta_bpr_basis_his`;

    -- Step 2: Reload historical basis product instances
    INSERT INTO `your-gcp-project.sof.ta_bpr_basis_his` (
      cntrct_id,
      bpr_id,
      bpri_com_id,
      iccid,
      imsi_mcc,
      imsi_mnc,
      imsi_hlr,
      imsi_si,
      cntrct_id_ref,
      valid_from,
      valid_to,
      modified_at,
      insert_at,
      slave_number,
      e_id
    )
    SELECT
      bp.cntrct_id,
      bp.bpr_id,
      bp.bpri_com_id,
      CONCAT(
        CAST(bp.iccid_mi AS STRING), '-',
        CAST(bp.iccid_ii AS STRING), '-',
        CAST(bp.iccid_iai AS STRING), '-',
        CAST(bp.iccid_nr AS STRING), '-',
        CAST(bp.iccid_cd AS STRING)
      ) AS iccid,
      bp.imsi_mcc,
      bp.imsi_mnc,
      bp.imsi_hlr,
      bp.imsi_si,
      bp.cntrct_id_ref,
      bp.valid_from,
      bp.valid_to,
      bp.modified_at,
      bp.insert_at,
      bp.slave_number,
      bp.eid AS e_id
    FROM `your-gcp-project.cds.ta_cntrct` c
    JOIN `your-gcp-project.pds.ta_bpri_com` bp
      ON c.cntrct_id = bp.cntrct_id
    WHERE c.cntrct_st IN (5, 6)        -- nur Vertragsstatus aktiv und beendet (d.h. reaktivierbar)
      AND c.redundant_owner_id = 1      -- keine Service Provider Vertraege
      AND c.insert_at <= v_process_date
      AND (c.modified_at IS NULL OR c.modified_at > v_process_date)
      AND c.valid_from <= v_process_date
      AND (c.valid_to IS NULL OR c.valid_to > v_process_date)
      AND c.is_production = 1
      AND (c.cntrct_ty NOT IN (1, 2, 5) OR c.cntrct_parent IS NOT NULL)
      AND c.cntrct_id = bp.cntrct_id
      AND bp.bpr_id IN (31, 2759, 2800, 2835, 2836, 2837, 3848) -- MultiSIM
      AND bp.insert_at <= v_process_date
      AND (bp.modified_at IS NULL OR bp.modified_at > v_process_date)
      AND bp.valid_from <= v_process_date
      AND bp.is_production = 1;

    SET v_rows_inserted = @@row_count;

    -- Return a status row. In Airflow, this would usually be read by a subsequent task
    -- or the procedure itself would log to a dedicated control table.
    SELECT
      'SUCCESS' AS status,
      v_process_date AS process_date,
      v_rows_inserted AS rows_inserted;

  EXCEPTION WHEN ERROR THEN
    -- Error handling: Log and re-raise the error.
    -- In a real scenario, you might also insert error details into a log table.
    SELECT
      'FAILED' AS status,
      v_process_date AS process_date,
      @@error.message AS error_message;
    RAISE USING MESSAGE = CONCAT(
      'Procedure `your-gcp-project.sof.d_ausd_bp_ta_bpr_basis_his` failed: ',
      @@error.message
    );
  END;
END;