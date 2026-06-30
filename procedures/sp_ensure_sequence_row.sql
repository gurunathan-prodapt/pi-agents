CREATE OR REPLACE PROCEDURE `${GCP_PROJECT_ID}.${BQ_DATASET_ID}.sp_ensure_sequence_row`(
  IN p_control_key STRING,
  IN p_start_value INT64
)
BEGIN
  MERGE `${GCP_PROJECT_ID}.${BQ_DATASET_ID}.message_entry_sequence` T
  USING (
    SELECT p_control_key AS control_key, p_start_value AS next_value
  ) S
  ON T.control_key = S.control_key
  WHEN NOT MATCHED THEN
    INSERT (control_key, next_value, created_at, updated_at)
    VALUES (S.control_key, S.next_value, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());
END;