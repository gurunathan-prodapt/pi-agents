-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_evn.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_evn.ksh

CREATE OR REPLACE PROCEDURE `your_gcp_project.your_bigquery_dataset.ausd_bp_ta_bpr_evn_core`(
  p_stichtag DATE,
  p_wiederanlaufWert INT64
)
BEGIN
  DECLARE v_active_cnt INT64 DEFAULT 0;

  -- Error handling
  BEGIN
    -- Check whether any active contract cache records exist for the given stichtag
    SELECT COUNT(1)
      INTO v_active_cnt
    FROM `your_gcp_project.your_bigquery_dataset.contract_cache_source`
    WHERE Gueltig_von <= p_stichtag
      AND p_stichtag < Gueltig_bis
      AND LADEDATUM < p_stichtag;

    -- If no active records exist, delete all target rows for the stichtag and stop
    IF v_active_cnt = 0 THEN
      DELETE FROM `your_gcp_project.your_bigquery_dataset.fos_target_table`
      WHERE stichtag = p_stichtag;

    ELSE
      -- Conditional restart delete
      IF p_wiederanlaufWert > 0 THEN
        DELETE FROM `your_gcp_project.your_bigquery_dataset.fos_target_table`
        WHERE DWH_VERTRAG_ID >= p_wiederanlaufWert
          AND stichtag = p_stichtag;
      END IF;

      -- Main insert
      INSERT INTO `your_gcp_project.your_bigquery_dataset.fos_target_table`
      SELECT
        src.* EXCEPT(contract_data_field_1, contract_data_field_2), -- Exclude dummy fields if you only want to insert the ones listed in fos_target_table
        src.contract_data_field_1,
        src.contract_data_field_2,
        p_stichtag AS stichtag
      FROM `your_gcp_project.your_bigquery_dataset.contract_cache_source` AS src
      WHERE src.Gueltig_von <= p_stichtag
        AND p_stichtag < src.Gueltig_bis
        AND src.LADEDATUM < p_stichtag;
    END IF;

  EXCEPTION WHEN ERROR THEN
    -- Re-raise with a meaningful message
    RAISE USING MESSAGE = CONCAT(
      'Error in ausd_bp_ta_bpr_evn_core for stichtag=',
      CAST(p_stichtag AS STRING),
      ', p_wiederanlaufWert=',
      CAST(p_wiederanlaufWert AS STRING)
    );
  END;
END;