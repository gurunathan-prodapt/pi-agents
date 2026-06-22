-- BigQuery Stored Procedure for environment variable initialization
-- Legacy source: vobs/dw_source/istools/seu/template/.dw_global
-- Job: vobs/dw_source/istools/seu/template/.dw_global

-- This stored procedure replicates the logic of the original KornShell script
-- to validate, compute, and return environment-like configuration values.
-- It accepts all required "environment variables" as input parameters and
-- returns a single-row table with the computed and static configuration values.
-- Replace `your_project_id.your_dataset_name` with your actual BigQuery project and dataset.
CREATE OR REPLACE PROCEDURE `your_project_id.your_dataset_name.dw_global_init`(
  p_dw_dir_root STRING,
  p_dw_dir_prot STRING,
  p_dw_dir_cubes STRING,
  p_dw_dir_imp_d1 STRING,
  p_dw_dir_imp_xtra STRING,
  p_dw_dir_imp_ctel STRING,
  p_oracle_home STRING,
  p_existing_ld_library_path STRING,
  p_existing_path STRING,
  p_cognos_setup_exists BOOL
)
RETURNS TABLE (
  dw_dir_root STRING,
  dw_dir_prot STRING,
  dw_dir_cubes STRING,
  dw_dir_imp_d1 STRING,
  dw_dir_imp_xtra STRING,
  dw_dir_imp_ctel STRING,
  oracle_home STRING,
  computed_ld_library_path STRING,
  computed_path STRING,
  nls_lang STRING,
  nls_date_format STRING,
  nls_date_language STRING,
  cognos_note STRING
)
BEGIN
  DECLARE fehler_list STRING DEFAULT '';
  DECLARE v_cognos_note STRING;

  -- 1. Environment Variable Validation: Check if critical input variables are set.
  -- If any are missing or empty, append to the error list.
  IF p_dw_dir_root IS NULL OR p_dw_dir_root = '' THEN
    SET fehler_list = CONCAT(fehler_list, ' DW_DIR_ROOT ');
  END IF;
  IF p_dw_dir_prot IS NULL OR p_dw_dir_prot = '' THEN
    SET fehler_list = CONCAT(fehler_list, ' DW_DIR_PROT ');
  END IF;
  IF p_dw_dir_cubes IS NULL OR p_dw_dir_cubes = '' THEN
    SET fehler_list = CONCAT(fehler_list, ' DW_DIR_CUBES ');
  END IF;
  IF p_dw_dir_imp_d1 IS NULL OR p_dw_dir_imp_d1 = '' THEN
    SET fehler_list = CONCAT(fehler_list, ' DW_DIR_IMP_D1 ');
  END IF;
  IF p_dw_dir_imp_xtra IS NULL OR p_dw_dir_imp_xtra = '' THEN
    SET fehler_list = CONCAT(fehler_list, ' DW_DIR_IMP_XTRA ');
  END IF;
  IF p_dw_dir_imp_ctel IS NULL OR p_dw_dir_imp_ctel = '' THEN
    SET fehler_list = CONCAT(fehler_list, ' DW_DIR_IMP_CTEL ');
  END IF;
  IF p_oracle_home IS NULL OR p_oracle_home = '' THEN
    SET fehler_list = CONCAT(fehler_list, ' ORACLE_HOME ');
  END IF;

  -- If any critical variables were missing, raise an exception.
  IF fehler_list IS NOT NULL AND fehler_list != '' THEN
    RAISE USING MESSAGE = CONCAT(
      'Fehler in .dw_global: ',
      'Umgebungsvariable(n) nicht gesetzt: ',
      TRIM(fehler_list),
      ' Breche ab ..'
    );
  END IF;

  -- 2. Path Derivation: Compute LD_LIBRARY_PATH and PATH based on Oracle Home.
  DECLARE v_computed_ld_library_path STRING;
  DECLARE v_computed_path STRING;

  -- Original: LD_LIBRARY_PATH=${ORACLE_HOME}/lib:${LD_LIBRARY_PATH}
  SET v_computed_ld_library_path = CONCAT(p_oracle_home, '/lib', IF(p_existing_ld_library_path IS NOT NULL AND p_existing_ld_library_path != '', CONCAT(':', p_existing_ld_library_path), ''));

  -- Original: PATH="$PATH:$ORACLE_HOME/bin:"
  SET v_computed_path = CONCAT(IF(p_existing_path IS NOT NULL AND p_existing_path != '', CONCAT(p_existing_path, ':'), ''), p_oracle_home, '/bin:');

  -- 3. NLS Settings: Set static NLS variables.
  DECLARE v_nls_lang STRING DEFAULT 'GERMAN_GERMANY.WE8ISO8859P1';
  DECLARE v_nls_date_format STRING DEFAULT 'DD-MON-YY';
  DECLARE v_nls_date_language STRING DEFAULT 'AMERICAN';

  -- 4. Cognos PowerPlay Sourcing: If cognos_setup_exists flag is true, set a note for external orchestration.
  IF p_cognos_setup_exists THEN
    SET v_cognos_note = 'Cognos setup script exists; external orchestration must apply its effects.';
  ELSE
    SET v_cognos_note = NULL;
  END IF;

  -- Return all computed and validated values as a single-row table.
  RETURN SELECT
    p_dw_dir_root AS dw_dir_root,
    p_dw_dir_prot AS dw_dir_prot,
    p_dw_dir_cubes AS dw_dir_cubes,
    p_dw_dir_imp_d1 AS dw_dir_imp_d1,
    p_dw_dir_imp_xtra AS dw_dir_imp_xtra,
    p_dw_dir_imp_ctel AS dw_dir_imp_ctel,
    p_oracle_home AS oracle_home,
    v_computed_ld_library_path AS computed_ld_library_path,
    v_computed_path AS computed_path,
    v_nls_lang AS nls_lang,
    v_nls_date_format AS nls_date_format,
    v_nls_date_language AS nls_date_language,
    v_cognos_note AS cognos_note;

END;