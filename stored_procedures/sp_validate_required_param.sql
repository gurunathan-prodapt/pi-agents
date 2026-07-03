-- File: stored_procedures/sp_validate_required_param.sql
-- Reusable validation procedure

CREATE OR REPLACE PROCEDURE `gcp-project-placeholder.dw_isbert_dataset.sp_validate_required_param`(
  IN p_param_name STRING,
  IN p_param_value STRING,
  OUT o_err_nr INT64,
  OUT o_err_arg STRING,
  OUT o_err_text STRING
)
BEGIN
  SET o_err_nr = 0;
  SET o_err_arg = '';
  SET o_err_text = '';

  IF p_param_value IS NULL OR TRIM(p_param_value) = '' THEN
    SET o_err_nr = 1;
    SET o_err_arg = p_param_name;
    SET o_err_text = 'Bitte ueber Rahmenscript aufrufen';
  END IF;
END;