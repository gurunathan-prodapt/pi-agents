--
-- BigQuery Stored Procedure: dw_global_init
-- Legacy Source: vobs/dw_source/istools/seu/template/.dw_global
-- Job: vobs/dw_source/istools/seu/template/.dw_global
--
-- Description: This procedure validates and derives environment configuration values
--              originally set by the .dw_global KornShell script.
--              It checks for the presence of critical directory paths and Oracle Home,
--              then constructs new values for LD_LIBRARY_PATH and PATH, and sets NLS
--              parameters. Errors are raised if required parameters are missing.
--
-- Usage: CALL `your-gcp-project-id.your_dataset_name.dw_global_init`(
--            p_dw_dir_root => '...', p_dw_dir_prot => '...', ...
--        );
--
-- The returned SELECT statement provides the derived configuration values.
--

CREATE OR REPLACE PROCEDURE `project.dataset.dw_global_init`(
    p_dw_dir_root STRING,
    p_dw_dir_prot STRING,
    p_dw_dir_cubes STRING,
    p_dw_dir_imp_d1 STRING,
    p_dw_dir_imp_xtra STRING,
    p_dw_dir_imp_ctel STRING,
    p_oracle_home STRING,
    p_initial_ld_library_path STRING, -- The LD_LIBRARY_PATH value before this script's changes
    p_initial_path STRING              -- The PATH value before this script's changes
)
OPTIONS(
    description="Validates and derives environment configuration values from legacy .dw_global script."
)
BEGIN
    DECLARE missing_vars ARRAY<STRING>;
    DECLARE error_message STRING;
    DECLARE v_ld_library_path STRING;
    DECLARE v_path STRING;

    SET missing_vars = [];

    -- Validate required DW_DIR_* variables
    IF p_dw_dir_root IS NULL OR TRIM(p_dw_dir_root) = '' THEN
        SET missing_vars = ARRAY_CONCAT(missing_vars, ['DW_DIR_ROOT']);
    END IF;

    IF p_dw_dir_prot IS NULL OR TRIM(p_dw_dir_prot) = '' THEN
        SET missing_vars = ARRAY_CONCAT(missing_vars, ['DW_DIR_PROT']);
    END IF;

    IF p_dw_dir_cubes IS NULL OR TRIM(p_dw_dir_cubes) = '' THEN
        SET missing_vars = ARRAY_CONCAT(missing_vars, ['DW_DIR_CUBES']);
    END IF;

    IF p_dw_dir_imp_d1 IS NULL OR TRIM(p_dw_dir_imp_d1) = '' THEN
        SET missing_vars = ARRAY_CONCAT(missing_vars, ['DW_DIR_IMP_D1']);
    END IF;

    IF p_dw_dir_imp_xtra IS NULL OR TRIM(p_dw_dir_imp_xtra) = '' THEN
        SET missing_vars = ARRAY_CONCAT(missing_vars, ['DW_DIR_IMP_XTRA']);
    END IF;

    IF p_dw_dir_imp_ctel IS NULL OR TRIM(p_dw_dir_imp_ctel) = '' THEN
        SET missing_vars = ARRAY_CONCAT(missing_vars, ['DW_DIR_IMP_CTEL']);
    END IF;

    -- Validate ORACLE_HOME
    IF p_oracle_home IS NULL OR TRIM(p_oracle_home) = '' THEN
        SET missing_vars = ARRAY_CONCAT(missing_vars, ['ORACLE_HOME']);
    END IF;

    -- If any required variables are missing, raise an error
    IF ARRAY_LENGTH(missing_vars) > 0 THEN
        SET error_message = CONCAT(
            'ERROR: Procedure aborted due to missing environment configuration variables. ',
            'The following parameters were not set or were empty: ',
            ARRAY_TO_STRING(missing_vars, ', ')
        );
        RAISE USING MESSAGE = error_message;
    END IF;

    -- Derive LD_LIBRARY_PATH: Prepend $ORACLE_HOME/lib to the initial path
    SET v_ld_library_path = CONCAT(p_oracle_home, '/lib',
                                   CASE WHEN p_initial_ld_library_path IS NOT NULL AND TRIM(p_initial_ld_library_path) != ''
                                        THEN CONCAT(':', p_initial_ld_library_path)
                                        ELSE ''
                                   END);

    -- Derive PATH: Prepend $ORACLE_HOME/bin to the initial path
    SET v_path = CONCAT(p_oracle_home, '/bin',
                        CASE WHEN p_initial_path IS NOT NULL AND TRIM(p_initial_path) != ''
                             THEN CONCAT(':', p_initial_path)
                             ELSE ''
                        END);

    -- Return the derived configuration values.
    -- These values can be consumed by an orchestration layer or subsequent BigQuery tasks.
    SELECT
        p_dw_dir_root AS dw_dir_root,
        p_dw_dir_prot AS dw_dir_prot,
        p_dw_dir_cubes AS dw_dir_cubes,
        p_dw_dir_imp_d1 AS dw_dir_imp_d1,
        p_dw_dir_imp_xtra AS dw_dir_imp_xtra,
        p_dw_dir_imp_ctel AS dw_dir_imp_ctel,
        p_oracle_home AS oracle_home,
        v_ld_library_path AS ld_library_path,
        v_path AS path,
        'AMERICAN_AMERICA.WE8ISO8859P1' AS nls_lang,
        'YYYY-MM-DD HH24:MI:SS' AS nls_date_format,
        'AMERICAN' AS nls_date_language;

END;