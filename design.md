# Migration Design — vobs/dw_source/isrpt/isbert/aufbereitung/bin/r_ausd_v_ta_action_assoc.ksh

## 1. Purpose & Scope
This document outlines the migration design for the KornShell script `r_ausd_v_ta_action_assoc.ksh`. This script serves as a wrapper or orchestration layer for a contract data reconciliation process, specifically for the `ta_action_assoc` table. Its primary functions include:
*   Setting up the runtime environment by sourcing utility scripts.
*   Parsing command-line parameters, primarily for help display.
*   Initializing job-specific logging and error handling mechanisms.
*   Invoking a core KornShell script, `k_ausd_v_ta_action_assoc.ksh`, which is expected to contain the main business logic.
*   Managing script execution flow, including error traps and final status reporting.

The script itself does not contain direct business transformation logic or data manipulation; it acts purely as a control flow mechanism for a dependent kernel script.

## 2. Source Inventory
The job consists of a single KornShell script.
*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_action_assoc.ksh`
    *   **Technology:** KornShell (shell script)
    *   **Category:** shell
    *   **Purpose:** Orchestration/Wrapper script
    *   **Complexity Tier:** Undetermined (file_complexity data not available)
    *   **Automation Bucket:** semi_auto

## 3. Target Architecture
The migration target platform is Google BigQuery.
The orchestration logic currently encapsulated in the KornShell script will be primarily migrated to a BigQuery Stored Procedure. This procedure will manage parameter validation, job metadata, logging, and the invocation of the core data processing logic.

The overall target architecture will consist of:
*   **BigQuery Stored Procedure:** `project.dataset.vertragsdatenabgleich` (or a similar naming convention) to handle the wrapper logic (parameter parsing, logging, orchestration).
*   **BigQuery Stored Procedure:** `project.dataset.k_ausd_v_ta_action_assoc` (or similar) which will encapsulate the migrated logic of the original `k_ausd_v_ta_action_assoc.ksh` script. This is an inferred dependency and its migration needs separate analysis.
*   **BigQuery Logging/Audit Tables:** To replace the file-based logging (`job_log`, `job_error_log`).
*   **External Orchestration (Optional):** If the core kernel script (`k_ausd_v_ta_action_assoc.ksh`) is migrated to a non-BigQuery component (e.g., PySpark in Dataproc), Cloud Composer or Cloud Workflows may be used to orchestrate the BigQuery stored procedure and any external components.

## 4. Data Flow & Lineage
The original script's data flow is primarily control flow and logging, delegating data processing to a subordinate script.
**Original Flow:**
1.  `r_ausd_v_ta_action_assoc.ksh` (Wrapper script)
    *   Sources environment/utility scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`).
    *   Parses command-line arguments.
    *   Initializes job metadata and logging via `DWMSG_*` functions.
    *   Executes `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_action_assoc.ksh` (Core kernel script) with parameters `-j $JobKennung -f ${DW_EintragsNr}`.
    *   Logs success or error status.

**Migrated Flow (BigQuery-centric):**
1.  **Orchestration Layer:**
    *   A BigQuery Stored Procedure (`project.dataset.vertragsdatenabgleich`) will serve as the entry point.
    *   This procedure will receive parameters (e.g., `p_jobkennung`, `p_run_date`, `p_enable_help`) either directly or from an external orchestrator.
    *   Parameter validation and initial job logging will occur within this procedure, writing to dedicated BigQuery logging tables.
    *   Error handling will use BigQuery's `EXCEPTION WHEN ERROR` blocks.
    *   This procedure will `CALL` the core processing BigQuery Stored Procedure (`project.dataset.k_ausd_v_ta_action_assoc`).
    *   Upon completion of the core procedure, it will update the job status in the BigQuery logging tables.

2.  **Core Processing Layer:**
    *   The `k_ausd_v_ta_action_assoc.ksh` script will be migrated to a separate BigQuery Stored Procedure (`project.dataset.k_ausd_v_ta_action_assoc`). This procedure will perform the actual data reconciliation for `ta_action_assoc`. The details of this migration are outside the scope of this document but are crucial for the overall job.

3.  **Logging:** All logging operations (job start, end, errors) will be directed to BigQuery audit/log tables, replacing file-based logging.

## 5. Transformation Logic
The `r_ausd_v_ta_action_assoc.ksh` script itself contains no direct data transformation logic. Its transformation logic is purely procedural and related to job orchestration:

*   **Environment Setup:** Sourcing of `.dw_init` and other utility scripts will be replaced by:
    *   BigQuery Stored Procedure parameters for dynamic values.
    *   Configuration tables within BigQuery for static environment variables.
    *   Direct inclusion of utility logic into the BigQuery Stored Procedure or separate helper procedures if they provide reusable SQL functionality.
*   **Parameter Parsing (`getopts`):** Replaced by:
    *   `IN` parameters of the BigQuery Stored Procedure.
    *   Conditional logic (`IF` statements) for validation.
    *   Error handling (`SIGNAL SQLSTATE`) for invalid parameters.
*   **Job Metadata & Logging (`DWMSG_*` functions):** Replaced by:
    *   `INSERT` statements into BigQuery logging/audit tables (`project.dataset.job_log`, `project.dataset.job_error_log`).
    *   BigQuery variables (`DECLARE`) for job numbers and log file names.
    *   `FORMAT_DATE` for date formatting.
*   **Error Handling (`trap`):** Replaced by:
    *   BigQuery scripting `BEGIN ... EXCEPTION WHEN ERROR THEN ... END;` blocks.
    *   `SIGNAL SQLSTATE` to raise exceptions.
*   **Kernel Script Execution (`${Name_Kernskript}`):** Replaced by:
    *   `CALL project.dataset.k_ausd_v_ta_action_assoc(v_jobkennung, DW_EintragsNr);` (assuming the kernel script is also migrated to a BigQuery Stored Procedure).

**Pseudocode for `project.dataset.vertragsdatenabgleich`:**
```sql
-- BigQuery Script / Stored Procedure Pseudocode
CREATE OR REPLACE PROCEDURE `project.dataset.vertragsdatenabgleich`(
  IN p_jobkennung STRING,
  IN p_run_date DATE,
  IN p_enable_help BOOL
)
BEGIN
  DECLARE ProgName STRING DEFAULT 'Vertragsdatenabgleich';
  DECLARE ProgVersion STRING DEFAULT 'V1.0.0';
  DECLARE ErrNr INT64 DEFAULT 0;
  DECLARE ErrArg STRING DEFAULT '';
  DECLARE DW_EintragsNr INT64 DEFAULT 0;
  DECLARE LogDatei STRING DEFAULT '';
  DECLARE v_sysdate STRING DEFAULT FORMAT_DATE('%d%m%Y', CURRENT_DATE());
  DECLARE Name_Kernskript STRING DEFAULT 'project.dataset.k_ausd_v_ta_action_assoc';
  DECLARE v_jobkennung STRING DEFAULT UPPER(COALESCE(p_jobkennung, 'BERT_V_TA_ACTION_ASSOC'));

  -- Help handling equivalent
  IF p_enable_help THEN
    SELECT
      ProgName AS Programm,
      ProgVersion AS Version,
      'Aufruf: CALL project.dataset.vertragsdatenabgleich(...)' AS Aufruf,
      'Rahmenskript fuer den Abgleich der Vertragsdaten: Tabelle ta_action_assoc.' AS Beschreibung;
    LEAVE;
  END IF;

  -- Parameter validation equivalent
  -- Example validation for p_jobkennung
  IF p_jobkennung IS NULL OR TRIM(p_jobkennung) = '' THEN
    SET ErrNr = 193;
    SET ErrArg = 'p_jobkennung';
  END IF;

  IF ErrNr != 0 THEN
    INSERT INTO `project.dataset.job_error_log`
      (job_name, entry_no, error_no, error_arg, created_ts)
    VALUES
      (ProgName, DW_EintragsNr, ErrNr, ErrArg, CURRENT_TIMESTAMP());
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = CONCAT('Parameterfehler: ', CAST(ErrNr AS STRING), ' ', ErrArg);
  END IF;

  -- Job metadata initialization
  -- Logic to generate DW_EintragsNr and LogDatei will be based on BigQuery tables
  -- For example, fetching max entry number from a log table
  SET DW_EintragsNr = (
    SELECT IFNULL(MAX(entry_no), 0) + 1
    FROM `project.dataset.job_log`
    WHERE job_kennung = v_jobkennung
  );

  SET LogDatei = CONCAT(v_jobkennung, '_', CAST(DW_EintragsNr AS STRING), '.log'); -- Placeholder, actual logging is to table

  INSERT INTO `project.dataset.job_log`
    (entry_no, job_kennung, program_name, program_version, log_name, status, stichtag, created_ts)
  VALUES
    (DW_EintragsNr, v_jobkennung, ProgName, ProgVersion, LogDatei, 'STARTED', v_sysdate, CURRENT_TIMESTAMP());

  BEGIN
    -- Equivalent to kernel script invocation
    CALL `project.dataset.k_ausd_v_ta_action_assoc`(v_jobkennung, DW_EintragsNr);

    -- Success handling
    INSERT INTO `project.dataset.job_log`
      (entry_no, job_kennung, program_name, program_version, log_name, status, stichtag, created_ts)
    VALUES
      (DW_EintragsNr, v_jobkennung, ProgName, ProgVersion, LogDatei, 'OK', v_sysdate, CURRENT_TIMESTAMP());

    SELECT 'Die Abarbeitung wurde ohne erkennbare Fehler beendet' AS message;

  EXCEPTION WHEN ERROR THEN
    -- Error handling and logging
    INSERT INTO `project.dataset.job_error_log`
      (entry_no, job_kennung, program_name, error_message, created_ts)
    VALUES
      (DW_EintragsNr, v_jobkennung, ProgName, @@error.message, CURRENT_TIMESTAMP());

    INSERT INTO `project.dataset.job_log`
      (entry_no, job_kennung, program_name, program_version, log_name, status, stichtag, created_ts)
    VALUES
      (DW_EintragsNr, v_jobkennung, ProgName, ProgVersion, LogDatei, 'ERROR', v_sysdate, CURRENT_TIMESTAMP());

    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'AppError: Abbruch';
  END;
END;
```

## 6. External Dependencies
The source script does not explicitly interact with external systems (like Oracle, SFTP, S3) directly. Its external dependencies are:
*   **Local Filesystem/Environment:**
    *   `$HOME/.dw_init`: An initialization script providing environment variables.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/f_alis_msgerr.ksh`: Error messaging utility.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_parameter.ksh`: Parameter handling utility.
    *   `${BERT_DIR_ROOT}/allgemein/is/util/bin/h_alis_date.ksh`: Date handling utility.
*   **Subordinate Script:**
    *   `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_action_assoc.ksh`: The core kernel script containing the actual data processing logic.

**Replacement in Target Architecture:**
*   **Environment Initialization (`.dw_init`):** Environment variables can be managed as BigQuery Stored Procedure parameters, configuration tables, or session variables.
*   **Utility Scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`):** The functionalities of these scripts (error logging, parameter validation, date formatting) will be re-implemented directly within the BigQuery Stored Procedure using BigQuery SQL scripting capabilities, or if complex, as separate helper BigQuery Stored Procedures.
*   **Core Kernel Script (`k_ausd_v_ta_action_assoc.ksh`):** This script is a critical dependency. Its migration plan must be developed separately. It is anticipated to be migrated to a BigQuery Stored Procedure, which will then be invoked via a `CALL` statement from the wrapper procedure. If it contains non-SQL logic, it might be migrated to a Cloud Function, Cloud Run service, or Dataflow job, and orchestrated by Cloud Composer or Cloud Workflows.

## 7. Unresolved / Risks
*   **Missing `file_complexity` data:** The complexity tier and migration flags for `r_ausd_v_ta_action_assoc.ksh` were not available. This prevents a granular understanding of specific migration challenges for this file beyond what was inferred by the MCP tool.
*   **`k_ausd_v_ta_action_assoc.ksh` Migration:** The primary risk and unresolved item is the migration of the core kernel script `k_ausd_v_ta_action_assoc.ksh`. This document only covers the wrapper. The actual data reconciliation logic will reside within this kernel script, and its migration strategy (BQ SQL, PySpark, etc.) will significantly impact the overall solution. A separate detailed design for `k_ausd_v_ta_action_assoc.ksh` is required.
*   **DWMSG Framework:** The `DWMSG_*` functions are a custom logging/error handling framework. While a BigQuery table-based approach is proposed, the exact mapping and preservation of all functionality (e.g., custom error codes, specific log formats) need careful consideration during implementation.
*   **Error Handling Fidelity:** Replicating the exact behavior of `trap INT` and `trap ERR` in shell scripts within BigQuery's `EXCEPTION WHEN ERROR` blocks requires careful testing to ensure equivalent robustness.
*   **Parameter Passing:** The original script uses `getopts`. The BigQuery stored procedure uses `IN` parameters. If the original script is called with complex or many parameters, this mapping needs to be robust.

## 8. Build Plan
The build plan focuses on the wrapper script's migration to a BigQuery Stored Procedure.

1.  **Define BigQuery Logging Tables:**
    *   **Language:** BigQuery DDL
    *   **Files:**
        *   `ddl/job_log_table.sql` (e.g., `CREATE TABLE project.dataset.job_log (...)`)
        *   `ddl/job_error_log_table.sql` (e.g., `CREATE TABLE project.dataset.job_error_log (...)`)

2.  **Migrate Utility Functionality:**
    *   **Language:** BigQuery SQL Scripting
    *   **Files:**
        *   `sp/util/f_alis_msgerr_impl.sql` (if error messaging logic is complex enough for a separate SP)
        *   `sp/util/h_alis_parameter_impl.sql` (if parameter logic is complex)
        *   `sp/util/h_alis_date_impl.sql` (if date logic is complex)
        *   *(Alternatively, embed simple utility logic directly into the main wrapper SP)*

3.  **Develop Core Kernel Stored Procedure (Placeholder/Dependency):**
    *   **Language:** BigQuery SQL (if fully SQL-migratable) or Python/Spark (if complex logic)
    *   **Files:** `sp/k_ausd_v_ta_action_assoc.sql` (or `dataproc/k_ausd_v_ta_action_assoc.py`)
    *   *Note: This is a placeholder and requires a separate design document.*

4.  **Develop Wrapper BigQuery Stored Procedure:**
    *   **Language:** BigQuery SQL Scripting
    *   **File:** `sp/vertragsdatenabgleich.sql`
    *   **Content:** The pseudocode provided in Section 5.

5.  **External Orchestration (if necessary):**
    *   **Language:** Python (for Airflow DAGs) or YAML (for Cloud Workflows)
    *   **File:** `orchestration/dag_vertragsdatenabgleich.py` (if using Cloud Composer)
    *   **Content:** Define the DAG to call the `project.dataset.vertragsdatenabgleich` BigQuery Stored Procedure.

This build plan assumes a staged migration where the wrapper is addressed first, with the core kernel script being an acknowledged dependency for a subsequent migration phase.