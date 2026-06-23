-- Migrates legacy source: vobs/dw_source/istools/seu/template/.dw_init and .dw_global
-- Job: vobs/dw_source/istools/seu/template/.dw_init

CREATE OR REPLACE PROCEDURE `your_project_id.your_dataset_id.dw_init_validate_config`(
    IN p_home_directory STRING,
    IN p_dw_dir_customer STRING,
    IN p_dw_host_customer STRING
)
OPTIONS(
    description="Encapsulates environment initialization logic from legacy .dw_init and .dw_global."
)
BEGIN
    -- Declare variables mirroring the original KornShell environment variables
    DECLARE DW_DIR_ROOT STRING;
    DECLARE DW_DIR_PROT STRING;
    DECLARE DW_DIR_CUBES STRING;
    DECLARE DW_DIR_IMP_D1 STRING;
    DECLARE DW_DIR_IMP_XTRA STRING;
    DECLARE DW_DIR_IMP_CTEL STRING;
    DECLARE DW_DIR_IMP_VO STRING;
    DECLARE DW_DIR_IMP_RV STRING;
    DECLARE DW_DIR_IMP_TRF STRING;
    DECLARE DW_DIR_IMP_TS STRING;
    DECLARE DW_DIR_IMP_ZM STRING;
    DECLARE DW_DIR_IMP_AUF STRING;
    DECLARE DW_DIR_IMP_GUT STRING;
    DECLARE DW_DIR_IMP_KDG STRING;
    DECLARE DW_DIR_IMP_MP_TS STRING;
    DECLARE DW_DIR_IMP_MP_KDG STRING;
    DECLARE DW_DIR_IMP_MP_ZM STRING;
    DECLARE DW_DIR_IMP_IF STRING;
    DECLARE DW_DIR_IMP_NNV STRING;
    DECLARE DW_DIR_IMP_CARMEN STRING;
    DECLARE GEN_HOME STRING;
    DECLARE ORACLE_HOME STRING;
    DECLARE NLS_LANG STRING;
    DECLARE NLS_DATE_FORMAT STRING;
    DECLARE NLS_DATE_LANGUAGE STRING;

    -- Set base directory variables, translating from $HOME to p_home_directory
    SET DW_DIR_ROOT = CONCAT(p_home_directory, '/aktuell');
    SET DW_DIR_PROT = CONCAT(p_home_directory, '/daten/logfiles');
    SET DW_DIR_CUBES = CONCAT(p_home_directory, '/daten/cubes');
    SET DW_DIR_IMP_D1 = CONCAT(p_home_directory, '/daten/d1');
    SET DW_DIR_IMP_XTRA = CONCAT(p_home_directory, '/daten/xtra');
    SET DW_DIR_IMP_CTEL = CONCAT(p_home_directory, '/daten/ctel');
    SET DW_DIR_IMP_VO = CONCAT(p_home_directory, '/daten/vo');
    SET DW_DIR_IMP_RV = CONCAT(p_home_directory, '/daten/rv');
    SET DW_DIR_IMP_TRF = CONCAT(p_home_directory, '/daten/trf');
    SET DW_DIR_IMP_TS = CONCAT(p_home_directory, '/daten/sd/ts');
    SET DW_DIR_IMP_ZM = CONCAT(p_home_directory, '/daten/sd/zm');
    SET DW_DIR_IMP_AUF = CONCAT(p_home_directory, '/daten/sd/auf');
    SET DW_DIR_IMP_GUT = CONCAT(p_home_directory, '/daten/sd/gut');
    SET DW_DIR_IMP_KDG = CONCAT(p_home_directory, '/daten/sd/kdg');
    SET DW_DIR_IMP_MP_TS = CONCAT(p_home_directory, '/daten/mp/ts');
    SET DW_DIR_IMP_MP_KDG = CONCAT(p_home_directory, '/daten/mp/kdg');
    SET DW_DIR_IMP_MP_ZM = CONCAT(p_home_directory, '/daten/mp/zm'); -- Original had typo: DW_DIR_IMP_MP_TS. Corrected to ZM based on pattern.
    SET DW_DIR_IMP_IF = CONCAT(p_home_directory, '/daten/if');
    SET DW_DIR_IMP_NNV = CONCAT(p_home_directory, '/daten/nnv');
    SET DW_DIR_IMP_CARMEN = CONCAT(p_home_directory, '/daten/carmen');

    SET GEN_HOME = CONCAT(DW_DIR_ROOT, '/generator');

    -- Remote System paths and hosts
    -- These are passed as parameters to allow dynamic values from an orchestrator
    -- SET DW_DIR_CUSTOMER = p_dw_dir_customer; -- The design document states this could be passed as a parameter
    -- SET DW_HOST_CUSTOMER = p_dw_host_customer; -- The design document states this could be passed as a parameter

    -- Determine ORACLE_HOME from the configuration table
    -- This replaces the filesystem probing logic from the original .dw_init
    SELECT candidate INTO ORACLE_HOME
    FROM `your_project_id.your_dataset_id.oracle_home_config`
    WHERE is_active IS TRUE
    ORDER BY priority DESC, candidate DESC -- Select highest priority active Oracle Home
    LIMIT 1;

    ASSERT ORACLE_HOME IS NOT NULL AND ORACLE_HOME != ''
        AS 'Error in dw_init_validate_config: ORACLE_HOME could not be determined from `oracle_home_config`.';

    -- Set NLS settings
    SET NLS_LANG = 'GERMAN_GERMANY.WE8ISO8859P1';
    SET NLS_DATE_FORMAT = 'DD-MON-YY';
    SET NLS_DATE_LANGUAGE = 'AMERICAN';

    -- Validation checks from .dw_global
    ASSERT DW_DIR_ROOT IS NOT NULL AND DW_DIR_ROOT != '' AS 'Environment variable DW_DIR_ROOT is not set!';
    ASSERT DW_DIR_PROT IS NOT NULL AND DW_DIR_PROT != '' AS 'Environment variable DW_DIR_PROT is not set!';
    ASSERT DW_DIR_CUBES IS NOT NULL AND DW_DIR_CUBES != '' AS 'Environment variable DW_DIR_CUBES is not set!';
    ASSERT DW_DIR_IMP_D1 IS NOT NULL AND DW_DIR_IMP_D1 != '' AS 'Environment variable DW_DIR_IMP_D1 is not set!';
    ASSERT DW_DIR_IMP_XTRA IS NOT NULL AND DW_DIR_IMP_XTRA != '' AS 'Environment variable DW_DIR_IMP_XTRA is not set!';
    ASSERT DW_DIR_IMP_CTEL IS NOT NULL AND DW_DIR_IMP_CTEL != '' AS 'Environment variable DW_DIR_IMP_CTEL is not set!';
    ASSERT ORACLE_HOME IS NOT NULL AND ORACLE_HOME != '' AS 'Environment variable ORACLE_HOME is not set!';
    ASSERT p_dw_dir_customer IS NOT NULL AND p_dw_dir_customer != '' AS 'Parameter p_dw_dir_customer is not set!';
    ASSERT p_dw_host_customer IS NOT NULL AND p_dw_host_customer != '' AS 'Parameter p_dw_host_customer is not set!';

    -- Persist the finalized configuration into dw_runtime_config
    BEGIN TRANSACTION;
    DELETE FROM `your_project_id.your_dataset_id.dw_runtime_config` WHERE TRUE; -- Clear previous configuration

    INSERT INTO `your_project_id.your_dataset_id.dw_runtime_config` (config_name, config_value)
    VALUES
        ('DW_DIR_ROOT', DW_DIR_ROOT),
        ('DW_DIR_PROT', DW_DIR_PROT),
        ('DW_DIR_CUBES', DW_DIR_CUBES),
        ('DW_DIR_IMP_D1', DW_DIR_IMP_D1),
        ('DW_DIR_IMP_XTRA', DW_DIR_IMP_XTRA),
        ('DW_DIR_IMP_CTEL', DW_DIR_IMP_CTEL),
        ('DW_DIR_IMP_VO', DW_DIR_IMP_VO),
        ('DW_DIR_IMP_RV', DW_DIR_IMP_RV),
        ('DW_DIR_IMP_TRF', DW_DIR_IMP_TRF),
        ('DW_DIR_IMP_TS', DW_DIR_IMP_TS),
        ('DW_DIR_IMP_ZM', DW_DIR_IMP_ZM),
        ('DW_DIR_IMP_AUF', DW_DIR_IMP_AUF),
        ('DW_DIR_IMP_GUT', DW_DIR_IMP_GUT),
        ('DW_DIR_IMP_KDG', DW_DIR_IMP_KDG),
        ('DW_DIR_IMP_MP_TS', DW_DIR_IMP_MP_TS),
        ('DW_DIR_IMP_MP_KDG', DW_DIR_IMP_MP_KDG),
        ('DW_DIR_IMP_MP_ZM', DW_DIR_IMP_MP_ZM),
        ('DW_DIR_IMP_IF', DW_DIR_IMP_IF),
        ('DW_DIR_IMP_NNV', DW_DIR_IMP_NNV),
        ('DW_DIR_IMP_CARMEN', DW_DIR_IMP_CARMEN),
        ('GEN_HOME', GEN_HOME),
        ('DW_DIR_CUSTOMER', p_dw_dir_customer),
        ('DW_HOST_CUSTOMER', p_dw_host_customer),
        ('ORACLE_HOME', ORACLE_HOME),
        ('NLS_LANG', NLS_LANG),
        ('NLS_DATE_FORMAT', NLS_DATE_FORMAT),
        ('NLS_DATE_LANGUAGE', NLS_DATE_LANGUAGE);

    -- Note on NLS_LANG, NLS_DATE_FORMAT, NLS_DATE_LANGUAGE:
    -- In BigQuery, these are session parameters. Storing them here allows downstream processes
    -- to retrieve and set them via 'SET NLS_DATE_FORMAT = (SELECT config_value FROM dw_runtime_config WHERE config_name = 'NLS_DATE_FORMAT');'

    -- Note on LD_LIBRARY_PATH and PATH:
    -- These are OS-level environment variables and do not have a direct BigQuery equivalent.
    -- If external tools require them, the orchestrator (e.g., Airflow) should construct
    -- these paths using the ORACLE_HOME value retrieved from this configuration.
    -- For BigQuery-native operations, these are not applicable.

    -- Note on .dw_lokal:
    -- The original .dw_init sourced a missing file named .dw_lokal.
    -- This stored procedure cannot replicate its functionality as its content is unknown.
    -- Any critical configurations or logic previously defined in .dw_lokal need to be
    -- identified and explicitly added to this procedure or managed externally.

    -- Note on Cognos PowerPlay setup:
    -- The original .dw_global conditionally sourced setpya.sh. This is an external script
    -- and cannot be executed within BigQuery. If Cognos PowerPlay is still in use,
    -- its environment setup must be handled by the orchestration layer.

    -- Note on umask 022:
    -- This is an OS-level file permission setting and has no direct BigQuery equivalent.
    -- Any file creation operations in the target environment should have their permissions
    -- managed by the respective GCP service configurations or by explicitly setting permissions
    -- in an external script or compute environment.

    COMMIT TRANSACTION;

END;