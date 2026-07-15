CREATE OR REPLACE PROCEDURE `GCP_PROJECT.BQ_DATASET.dw_global`(
  IN IN_DW_DIR_ROOT STRING,
  IN IN_DW_DIR_PROT STRING,
  IN IN_DW_DIR_CUBES STRING,
  IN IN_DW_DIR_IMP_D1 STRING,
  IN IN_DW_DIR_IMP_XTRA STRING,
  IN IN_DW_DIR_IMP_CTEL STRING,
  IN IN_DW_DIR_IMP_VO STRING,
  IN IN_DW_DIR_IMP_RV STRING,
  IN IN_DW_DIR_IMP_IF STRING,
  IN IN_DW_DIR_IMP_NNV STRING,
  IN IN_ORACLE_HOME STRING
)
BEGIN
  DECLARE fehler STRING DEFAULT '';

  -- 1. Validation of parameters mimicking environment check from source code
  IF IN_DW_DIR_ROOT IS NULL OR IN_DW_DIR_ROOT = '' THEN
    SET fehler = CONCAT(fehler, 'DW_DIR_ROOT ');
  END IF;
  IF IN_DW_DIR_PROT IS NULL OR IN_DW_DIR_PROT = '' THEN
    SET fehler = CONCAT(fehler, 'DW_DIR_PROT ');
  END IF;
  IF IN_DW_DIR_CUBES IS NULL OR IN_DW_DIR_CUBES = '' THEN
    SET fehler = CONCAT(fehler, 'DW_DIR_CUBES ');
  END IF;
  IF IN_DW_DIR_IMP_D1 IS NULL OR IN_DW_DIR_IMP_D1 = '' THEN
    SET fehler = CONCAT(fehler, 'DW_DIR_IMP_D1 ');
  END IF;
  IF IN_DW_DIR_IMP_XTRA IS NULL OR IN_DW_DIR_IMP_XTRA = '' THEN
    SET fehler = CONCAT(fehler, 'DW_DIR_IMP_XTRA ');
  END IF;
  IF IN_DW_DIR_IMP_CTEL IS NULL OR IN_DW_DIR_IMP_CTEL = '' THEN
    SET fehler = CONCAT(fehler, 'DW_DIR_IMP_CTEL ');
  END IF;
  IF IN_DW_DIR_IMP_VO IS NULL OR IN_DW_DIR_IMP_VO = '' THEN
    SET fehler = CONCAT(fehler, 'DW_DIR_IMP_VO ');
  END IF;
  IF IN_DW_DIR_IMP_RV IS NULL OR IN_DW_DIR_IMP_RV = '' THEN
    SET fehler = CONCAT(fehler, 'DW_DIR_IMP_RV ');
  END IF;
  IF IN_DW_DIR_IMP_IF IS NULL OR IN_DW_DIR_IMP_IF = '' THEN
    SET fehler = CONCAT(fehler, 'DW_DIR_IMP_IF ');
  END IF;
  IF IN_DW_DIR_IMP_NNV IS NULL OR IN_DW_DIR_IMP_NNV = '' THEN
    SET fehler = CONCAT(fehler, 'DW_DIR_IMP_NNV ');
  END IF;
  IF IN_ORACLE_HOME IS NULL OR IN_ORACLE_HOME = '' THEN
    SET fehler = CONCAT(fehler, 'ORACLE_HOME ');
  END IF;

  SET fehler = TRIM(fehler);
  
  -- Verbatim legacy output formatting constraint check
  IF LENGTH(fehler) > 0 THEN
    ERROR(CONCAT('Fehler in .dw_global:\nDie folgenden Umgebungsvariablen sind nicht gesetzt: ', fehler));
  END IF;
END;