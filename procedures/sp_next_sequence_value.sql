CREATE OR REPLACE PROCEDURE `${GCP_PROJECT_ID}.${BQ_DATASET_ID}.sp_next_sequence_value`(
  IN p_control_key STRING,
  OUT p_next_value INT64
)
BEGIN
  DECLARE v_current INT64;

  CALL `${GCP_PROJECT_ID}.${BQ_DATASET_ID}.sp_ensure_sequence_row`(p_control_key, 1);

  SET v_current = (
    SELECT next_value
    FROM `${GCP_PROJECT_ID}.${BQ_DATASET_ID}.message_entry_sequence`
    WHERE control_key = p_control_key
  );

  UPDATE `${GCP_PROJECT_ID}.${BQ_DATASET_ID}.message_entry_sequence`
  SET next_value = next_value + 1,
      updated_at = CURRENT_TIMESTAMP()
  WHERE control_key = p_control_key;

  SET p_next_value = v_current;
END;