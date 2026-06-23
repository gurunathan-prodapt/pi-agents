-- BigQuery SQL script to initialize DW environment variables
-- Legacy source: vobs/dw_source/istools/seu/template/.dw_init
-- Job: vobs/dw_source/istools/seu/template/.dw_init

CREATE OR REPLACE PROCEDURE `project.dataset.init_dw_environment`(
    home_path STRING,
    login_placeholder STRING,
    initial_oracle_home STRING, -- Optional: if ORACLE_HOME is already set externally
    oracle_exists_816 BOOL,
    oracle_exists_734 BOOL,
    oracle_exists_733 BOOL,
    oracle_exists_732 BOOL,
    oracle_exists_723 BOOL
)
BEGIN
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
    DECLARE DW_DIR_CUSTOMER STRING;
    DECLARE DW_HOST_CUSTOMER STRING;
    DECLARE ORACLE_HOME_VAR STRING;

    -- Set base directory variables
    SET DW_DIR_ROOT = CONCAT(home_path, '/aktuell');
    SET DW_DIR_PROT = CONCAT(home_path, '/daten/logfiles');
    SET DW_DIR_CUBES = CONCAT(home_path, '/daten/cubes');
    SET DW_DIR_IMP_D1 = CONCAT(home_path, '/daten/d1');
    SET DW_DIR_IMP_XTRA = CONCAT(home_path, '/daten/xtra');
    SET DW_DIR_IMP_CTEL = CONCAT(home_path, '/daten/ctel');
    SET DW_DIR_IMP_VO = CONCAT(home_path, '/daten/vo');
    SET DW_DIR_IMP_RV = CONCAT(home_path, '/daten/rv');
    SET DW_DIR_IMP_TRF = CONCAT(home_path, '/daten/trf');
    SET DW_DIR_IMP_TS = CONCAT(home_path, '/daten/sd/ts');
    SET DW_DIR_IMP_ZM = CONCAT(home_path, '/daten/sd/zm');
    SET DW_DIR_IMP_AUF = CONCAT(home_path, '/daten/sd/auf');
    SET DW_DIR_IMP_GUT = CONCAT(home_path, '/daten/sd/gut');
    SET DW_DIR_IMP_KDG = CONCAT(home_path, '/daten/sd/kdg');
    SET DW_DIR_IMP_MP_TS = CONCAT(home_path, '/daten/mp/ts');
    SET DW_DIR_IMP_MP_KDG = CONCAT(home_path, '/daten/mp/kdg');
    SET DW_DIR_IMP_MP_ZM = CONCAT(home_path, '/daten/mp/zm'); -- Corrected: original ksh exported DW_DIR_IMP_MP_TS here
    SET DW_DIR_IMP_IF = CONCAT(home_path, '/daten/if');
    SET DW_DIR_IMP_NNV = CONCAT(home_path, '/daten/nnv');
    SET DW_DIR_IMP_CARMEN = CONCAT(home_path, '/daten/carmen');

    SET GEN_HOME = CONCAT(DW_DIR_ROOT, '/generator');

    SET DW_DIR_CUSTOMER = login_placeholder;
    SET DW_HOST_CUSTOMER = 'dxcst3.bn.detemobil.de';

    -- ORACLE_HOME resolution logic
    SET ORACLE_HOME_VAR = initial_oracle_home;

    IF ORACLE_HOME_VAR IS NULL OR ORACLE_HOME_VAR = '' THEN
        IF oracle_exists_816 THEN
            SET ORACLE_HOME_VAR = '/appl/local/oracle/8.1.6';
        ELSEIF oracle_exists_734 THEN
            SET ORACLE_HOME_VAR = '/appl/local/oracle/7.3.4';
        ELSEIF oracle_exists_733 THEN
            SET ORACLE_HOME_VAR = '/appl/local/oracle/oracle.7.3.3';
        ELSEIF oracle_exists_732 THEN
            SET ORACLE_HOME_VAR = '/appl/local/oracle/7.3.2';
        ELSEIF oracle_exists_723 THEN
            SET ORACLE_HOME_VAR = '/appl/local/oracle/7.2.3';
        ELSE
            RAISE USING MESSAGE = 'Fehler in init_dw_environment: Konnte ORACLE_HOME nicht setzen ! Aborting.';
        END IF;
    END IF;

    -- This SELECT statement outputs the resolved environment variables.
    -- In a real orchestration scenario, these values might be used directly
    -- within the same script or by a calling procedure.
    SELECT
        DW_DIR_ROOT,
        DW_DIR_PROT,
        DW_DIR_CUBES,
        DW_DIR_IMP_D1,
        DW_DIR_IMP_XTRA,
        DW_DIR_IMP_CTEL,
        DW_DIR_IMP_VO,
        DW_DIR_IMP_RV,
        DW_DIR_IMP_TRF,
        DW_DIR_IMP_TS,
        DW_DIR_IMP_ZM,
        DW_DIR_IMP_AUF,
        DW_DIR_IMP_GUT,
        DW_DIR_IMP_KDG,
        DW_DIR_IMP_MP_TS,
        DW_DIR_IMP_MP_KDG,
        DW_DIR_IMP_MP_ZM,
        DW_DIR_IMP_IF,
        DW_DIR_IMP_NNV,
        DW_DIR_IMP_CARMEN,
        GEN_HOME,
        DW_DIR_CUSTOMER,
        DW_HOST_CUSTOMER,
        ORACLE_HOME_VAR AS ORACLE_HOME;

    -- To load configurations from dw_global_config and dw_lokal_config:
    -- You would typically query these tables and use the values in subsequent steps
    -- or pass them to other procedures/functions.
    -- For demonstration, we'll just show selecting them.
    SELECT * FROM `project.dataset.dw_global_config`;
    SELECT * FROM `project.dataset.dw_lokal_config`;

END;