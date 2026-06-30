CREATE OR REPLACE PROCEDURE `${GCP_PROJECT_ID}.${BQ_DATASET_ID}.sp_update_message_status`(
  IN p_EintragsNr INT64,
  IN p_Status STRING
)
BEGIN
  UPDATE `${GCP_PROJECT_ID}.${BQ_DATASET_ID}.message_table`
  SET status = p_Status,
      updated_at = CURRENT_TIMESTAMP()
  WHERE eintragsnr = p_EintragsNr;
END;