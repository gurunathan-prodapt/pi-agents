# Migration Design — vobs/dw_source/istools/seu/template/.dw_global

## 1. Purpose & Scope
This migration job focuses on the `.dw_global` KornShell script, which is a foundational component for a Data Warehouse (DW) environment. Its primary purpose is to initialize global environment variables and system paths, ensuring that downstream processes have the necessary configurations. It includes checks for critical environment variables and conditionally sources a Cognos setup script. The script is designed to be sourced by a parent initialization script (`dwh_init`) rather than being executed directly. The overall job was assembled from this single component and is categorized as having a medium stage distribution.

## 2. Source Inventory
The migration job involves a single source file:

*   **File:** `vobs/dw_source/istools/seu/template/.dw_global`
    *   **Technology:** KornShell
    *   **Description:** Sets global environment variables and paths for a DW environment, performs checks for critical variables, and sources a Cognos setup script.
    *   **Complexity Tier:** medium
    *   **Automation Bucket:** semi_auto

## 3. Target Architecture
The functionality of the `.dw_global` script will be migrated to BigQuery. Since the script primarily handles environment variable setup and conditional logic, the target architecture will consist of:

*   **BigQuery Stored Procedure:** To encapsulate the validation logic and the assignment of logical environment variables. This stored procedure will take required environment values as input parameters and output the resolved configuration.
*   **External Orchestration (e.g., Cloud Composer/Workflows/Python Wrapper):** To manage the actual execution context, pass dynamic environment variables into the BigQuery Stored Procedure, and handle any OS-level operations (like checking for the existence of the Cognos setup script or applying Cognos-derived settings) that cannot be directly replicated in BigQuery SQL.
*   **Configuration Table (Optional):** A BigQuery table could be used to store default or dynamic configuration values, replacing some of the hardcoded paths or environmental dependencies for a more flexible approach.

## 4. Data Flow & Lineage
The `.dw_global` script does not involve traditional data ingestion or transformation operations. Its role is to configure the runtime environment for other scripts.

*   **Inputs:**
    *   Existing shell environment variables: `DW_DIR_ROOT`, `DW_DIR_PROT`, `DW_DIR_CUBES`, `DW_DIR_IMP_D1`, `DW_DIR_IMP_XTRA`, `DW_DIR_IMP_CTEL`, `ORACLE_HOME`, `LD_LIBRARY_PATH`, `PATH`. These will become explicit input parameters to the BigQuery Stored Procedure.
    *   Existence of external file: `/appl/local/cognos/cognos5.2/pya52b17/setpya.sh`. This check will be managed by external orchestration.
*   **Outputs:**
    *   Error messages printed to console (will be translated to BigQuery `SELECT` statements for logging or inserts into a log table).
    *   Exported environment variables: `LD_LIBRARY_PATH`, `PATH`, `NLS_LANG`, `NLS_DATE_FORMAT`, `NLS_DATE_LANGUAGE`. These will be returned as output parameters or as a result set from the BigQuery Stored Procedure, to be consumed by the orchestrator.
*   **Execution Order:** The script is primarily sourced by `dwh_init`, implying it's an early step in a broader ETL workflow, setting the stage for subsequent data processing scripts.

## 5. Transformation Logic
The core logic of the KornShell script will be translated into a BigQuery Stored Procedure.

### Script Breakdown
1.  **Validation:** The script checks if several critical `DW_DIR_*` and `ORACLE_HOME` environment variables are set. If any are missing, it accumulates error messages.
2.  **Path and NLS Settings:** It constructs `LD_LIBRARY_PATH` and `PATH` by prepending Oracle-specific directories and sets Oracle NLS settings (`NLS_LANG`, `NLS_DATE_FORMAT`, `NLS_DATE_LANGUAGE`).
3.  **Cognos Integration:** It conditionally sources an external `setpya.sh` script if it exists, integrating Cognos PowerPlay environment settings.
4.  **Commented Logic:** Sections for expanding `PATH` and `SQLPATH` are commented out in the source and will not be actively migrated unless identified as required by other components.

### BigQuery SQL Pseudocode
The following BigQuery Stored Procedure outlines the equivalent logic:

```sql
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
```

## 6. External Dependencies
The original `.dw_global` script has the following external dependencies:

*   **Oracle Client Environment (`ORACLE_HOME`, `LD_LIBRARY_PATH`, `PATH` with Oracle bins):** This script sets up paths relevant to an Oracle client installation. In BigQuery, direct Oracle client interaction is not applicable. The environment variables related to Oracle will be treated as configuration parameters. If there are downstream processes that require connecting to an Oracle database, these will need to be re-architected to use BigQuery's federation capabilities or external data sources (e.g., Cloud SQL for Oracle, or data ingestion pipelines into BigQuery). The `lineage_edges` indicated `EXT:DATABASE` (Oracle) is used by other scripts (`vobs/dw_source/isdwh/exporter/is/bin/r_exis_v2`), which suggests that direct database connectivity will need to be addressed.
*   **Cognos PowerPlay Setup Script (`/appl/local/cognos/cognos5.2/pya52b17/setpya.sh`):** The script conditionally sources this shell script. In the BigQuery environment, directly sourcing shell scripts is not possible. If the Cognos-specific environment settings are crucial for downstream processes, they will need to be managed by the external orchestration layer. The orchestration could either set these variables for Python scripts or other non-BigQuery components, or the relevant settings could be extracted and passed as parameters to BigQuery processes.

## 7. Unresolved / Risks
*   **Environment Mutation:** BigQuery SQL does not directly support mutation of the operating system environment. This functionality will be handled by an external orchestration layer that wraps the BigQuery Stored Procedure, interpreting its output to set environmental parameters for subsequent steps. This is why the migration is categorized as `semi_auto`.
*   **External Script Sourcing:** The conditional sourcing of `setpya.sh` is a shell-specific operation. The external orchestration layer will need to manage the existence check (`cognos_setup_exists` boolean parameter) and, if necessary, execute or replicate the effects of the Cognos setup script outside of BigQuery.
*   **Fatal Error Handling:** The original script identifies missing variables and prints messages, but does not explicitly `exit` with an error code. The BigQuery stored procedure should incorporate explicit error handling (e.g., `RAISE ERROR`) if any critical input parameters are missing, to ensure that the calling orchestration layer can react appropriately.
*   **Scope of `dwh_init`:** The current analysis is focused only on `.dw_global`. The full migration strategy will need to consider `dwh_init` and any other scripts that source `.dw_global` to ensure a holistic environment setup in the target.
*   **Commented Code:** The commented-out `PATH` and `SQLPATH` expansions are not being actively migrated. If these are ever uncommented or become active in the source, they would require re-evaluation.

## 8. Build Plan
1.  **Define BigQuery Stored Procedure:** Create the `project.dataset.dw_global_init` stored procedure in BigQuery, implementing the logic derived from the KornShell script.
2.  **Develop Orchestration Wrapper:**
    *   Create an orchestration script (e.g., Python script for Cloud Composer, or a Cloud Workflow definition).
    *   This wrapper will collect the necessary input parameters (e.g., `DW_DIR_ROOT`, `ORACLE_HOME`, current `PATH`, `LD_LIBRARY_PATH`).
    *   It will perform the file existence check for the Cognos `setpya.sh` script.
    *   It will call the BigQuery `dw_global_init` stored procedure, passing the inputs.
    *   It will capture the output parameters/result set from the stored procedure (e.g., `LD_LIBRARY_PATH_OUT`, `PATH_OUT`, `NLS_LANG_OUT`).
    *   It will then apply these output settings to the environment for subsequent steps in the pipeline or pass them to other components.
    *   Implement robust error handling in the orchestrator to catch errors from the BigQuery stored procedure and manage the "abort" condition.
3.  **Integrate Cognos Setup:** If the Cognos environment setup is critical, the orchestration layer will need to contain logic to replicate or execute the necessary Cognos settings. This might involve calling a separate script, loading configurations from a file, or directly setting environment variables within the orchestrator's context.
4.  **Testing:** Thoroughly test the BigQuery stored procedure and the orchestration wrapper to ensure environment variables are correctly validated and propagated, and that error conditions are handled gracefully.