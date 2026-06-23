-- Migrated from legacy source: vobs/dw_source/istools/seu/template/.dw_global
-- Job: vobs/dw_source/istools/seu/template/.dw_global

CREATE OR REPLACE PROCEDURE `project.dataset.dw_global_init`(
  IN DW_DIR_ROOT STRING,
  IN DW_DIR_PROT STRING,
  IN DW_DIR_CUBES STRING,
  IN DW_DIR_IMP_D1 STRING,
  IN DW_DIR_IMP_XTRA STRING,
  IN DW_DIR_IMP_CTEL STRING,
  IN ORACLE_HOME STRING,
  IN LD_LIBRARY_PATH_IN STRING,
  IN PATH_IN STRING,
  IN cognos_setup_exists BOOL
)
BEGIN
  DECLARE fehler STRING DEFAULT '';
  DECLARE missing_vars ARRAY<STRING> DEFAULT [];
  DECLARE LD_LIBRARY_PATH STRING;
  DECLARE PATH STRING;
  DECLARE NLS_LANG STRING DEFAULT 'GERMAN_GERMANY.WE8ISO8859P1';
  DECLARE NLS_DATE_FORMAT STRING DEFAULT 'DD-MON-YY';
  DECLARE NLS_DATE_LANGUAGE STRING DEFAULT 'AMERICAN';

  IF DW_DIR_ROOT IS NULL OR DW_DIR_ROOT = '' THEN
    SET fehler = CONCAT(fehler, ' DW_DIR_ROOT ');
    SET missing_vars = ARRAY_CONCAT(missing_vars, ['DW_DIR_ROOT']);
  END IF;

  IF DW_DIR_PROT IS NULL OR DW_DIR_PROT = '' THEN
    SET fehler = CONCAT(fehler, ' DW_DIR_PROT ');
    SET missing_vars = ARRAY_CONCAT(missing_vars, ['DW_DIR_PROT']);
  END IF;

  IF DW_DIR_CUBES IS NULL OR DW_DIR_CUBES = '' THEN
    SET fehler = CONCAT(fehler, ' DW_DIR_CUBES ');
    SET missing_vars = ARRAY_CONCAT(missing_vars, ['DW_DIR_CUBES']);
  END IF;

  IF DW_DIR_IMP_D1 IS NULL OR DW_DIR_IMP_D1 = '' THEN
    SET fehler = CONCAT(fehler, ' DW_DIR_IMP_D1 ');
    SET missing_vars = ARRAY_CONCAT(missing_vars, ['DW_DIR_IMP_D1']);
  END IF;

  IF DW_DIR_IMP_XTRA IS NULL OR DW_DIR_IMP_XTRA = '' THEN
    SET fehler = CONCAT(fehler, ' DW_DIR_IMP_XTRA');
    SET missing_vars = ARRAY_CONCAT(missing_vars, ['DW_DIR_IMP_XTRA']);
  END IF;

  IF DW_DIR_IMP_CTEL IS NULL OR DW_DIR_IMP_CTEL = '' THEN
    SET fehler = CONCAT(fehler, ' DW_DIR_IMP_CTEL');
    SET missing_vars = ARRAY_CONCAT(missing_vars, ['DW_DIR_IMP_CTEL']);
  END IF;

  IF ORACLE_HOME IS NULL OR ORACLE_HOME = '' THEN
    SET fehler = CONCAT(fehler, ' ORACLE_HOME');
    SET missing_vars = ARRAY_CONCAT(missing_vars, ['ORACLE_HOME']);
  END IF;

  IF fehler IS NOT NULL AND fehler != '' THEN
    SELECT 'Fehler in .dw_global:' AS message;

    FOR rec IN (
      SELECT varname
      FROM UNNEST(missing_vars) AS varname
    ) DO
      SELECT CONCAT('   Umgebungsvariable ', rec.varname, ' ist nicht gesetzt !') AS message;
    END FOR;

    SELECT 'Breche ab ..' AS message;
  END IF;

  SET LD_LIBRARY_PATH = CONCAT(ORACLE_HOME, '/lib:', IFNULL(LD_LIBRARY_PATH_IN, ''));
  SET PATH = CONCAT(IFNULL(PATH_IN, ''), ':', ORACLE_HOME, '/bin:');

  IF cognos_setup_exists THEN
    -- External orchestration required; no native BigQuery equivalent for sourcing shell scripts.
    -- Placeholder for externally supplied Cognos-derived settings.
    SELECT 'Cognos setup script detected; external setup must be applied outside BigQuery.' AS message;
  END IF;

  SELECT
    DW_DIR_ROOT AS DW_DIR_ROOT_OUT,
    DW_DIR_PROT AS DW_DIR_PROT_OUT,
    DW_DIR_CUBES AS DW_DIR_CUBES_OUT,
    DW_DIR_IMP_D1 AS DW_DIR_IMP_D1_OUT,
    DW_DIR_IMP_XTRA AS DW_DIR_IMP_XTRA_OUT,
    DW_DIR_IMP_CTEL AS DW_DIR_IMP_CTEL_OUT,
    ORACLE_HOME AS ORACLE_HOME_OUT,
    LD_LIBRARY_PATH AS LD_LIBRARY_PATH_OUT,
    PATH AS PATH_OUT,
    NLS_LANG AS NLS_LANG_OUT,
    NLS_DATE_FORMAT AS NLS_DATE_FORMAT_OUT,
    NLS_DATE_LANGUAGE AS NLS_DATE_LANGUAGE_OUT;
END;