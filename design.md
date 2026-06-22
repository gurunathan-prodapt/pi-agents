# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_opt_text.ksh

## 1. Purpose & Scope
This KornShell script (`r_ausd_bp_ta_bpr_opt_text.ksh`) serves as an orchestrator for the initial provisioning of selected basic products (e.g., FAX, Data24) for the BERT system. Its primary function is to handle parameter parsing, environment setup, date determination, and error logging before invoking a core transformation script. The job creates a snapshot extraction of contract cache data from DWH and makes it available for scoring/FOS (Forderungsscoring). It supports restart/resume behavior through a restart value and uses a default cutoff date when one is not explicitly provided.

## 2. Source Inventory
The job is comprised of a single KornShell script.
*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_opt_text.ksh`
    *   **Category:** shell
    *   **Tool:** KornShell
    *   **Complexity Tier:** Unknown (no data in `file_complexity` table)
    *   **Automation Bucket:** Unknown (no data in `automation_rate` table)

## 3. Target Architecture
The migration target is Google BigQuery. The legacy shell script, being an orchestrator, will be re-platformed as a BigQuery Stored Procedure.

*   **Main Wrapper Procedure:** A BigQuery Stored Procedure, `project.dataset.ausd_bp_ta_bpr_opt_text_wrapper`, will encapsulate the parameter handling, date defaulting, and logging logic of the original shell script. It will accept `p_stichtag` (cutoff date) and `p_wiederanlaufWert` (restart value) as input parameters.
*   **Core Logic Procedure:** A separate BigQuery Stored Procedure, `project.dataset.k_ausd_bp_ta_bpr_opt_text`, will contain the core data extraction and provisioning logic that was originally in `k_ausd_bp_ta_bpr_opt_text.ksh`. The wrapper procedure will invoke this core logic procedure.
*   **Audit/Log Table:** A dedicated BigQuery table (e.g., `project.dataset.job_log_audit`) will be used to record job execution status, parameters, timestamps, and error messages, replacing the file-based logging mechanism.
*   **Configuration:** Environment variables and sourced configuration files will be replaced by procedure parameters, a dataset-level configuration table, or session variables.

## 4. Data Flow & Lineage
The original script `r_ausd_bp_ta_bpr_opt_text.ksh` performs the following logical flow:
1.  **Environment and Helper Script Loading:** Sources several helper scripts for environment initialization, error handling, parameter parsing, and date functions (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`).
2.  **Parameter Parsing:** Reads command-line arguments `-s` (Stichtag/cutoff date) and `-l` (Wiederanlaufwert/restart value).
3.  **Defaulting and Validation:**
    *   Initializes `p_wiederanlaufWert` to `0` if not provided.
    *   Determines the system date.
    *   If `p_stichtag` is not provided, it defaults to the system date.
    *   Validates that `p_stichtag` is set. If validation fails, an error is logged, usage information is displayed, and the script exits.
4.  **Logging Setup:** Initializes job logging metadata, including a unique entry number (`DW_EintragsNr`) and a log file name (`LogDatei`).
5.  **Error Trapping:** Sets up shell `trap` commands to handle various signals (INT, STOP, CONT, ERR) for robust error management, invoking a centralized error handling function (`DWMSG_Fehlerbehandlung`).
6.  **Core Script Invocation:** Executes the core processing script `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_bp_ta_bpr_opt_text.ksh` with the parsed and defaulted parameters.
7.  **Completion Handling:** On successful execution of the core script, it logs a success message and updates the job status (`DWMSG_SetzeStatusOK`).

In BigQuery, this flow will translate to:
*   The `ausd_bp_ta_bpr_opt_text_wrapper` stored procedure will handle parameter input, defaulting, validation, and audit logging.
*   It will then call the `k_ausd_bp_ta_bpr_opt_text` stored procedure to perform the actual data processing.
*   Error handling will be managed using BigQuery's `EXCEPTION WHEN ERROR` block within the stored procedure.

## 5. Transformation Logic
The `r_ausd_bp_ta_bpr_opt_text.ksh` script primarily acts as an orchestration and parameter management layer, with minimal direct business transformation logic. Its main transformations involve:
*   **Parameter Normalization:**
    *   Defaulting `p_wiederanlaufWert` to `0` if not provided.
    *   Defaulting `p_stichtag` to the system date if not provided.
*   **Date Formatting:** Uses `DWDate_Gib_Zeitraum` to get the system date in `DDMMYYYY` format.

The core business logic (data extraction, filtering, and insertion into the FOS table) is delegated to the `k_ausd_bp_ta_bpr_opt_text.ksh` script.

**Migration of Constructs to BigQuery SQL:**

| Bash Construct            | BigQuery SQL Equivalent / Approach                                  |
| :------------------------ | :------------------------------------------------------------------ |
| Environment Sourcing      | Procedure parameters, config tables, session variables              |
| `getopts` parameter parsing | Stored procedure input parameters                                   |
| `if [[ -z ... ]]`         | `IF condition THEN SET variable END IF;` or `IFNULL()`              |
| `print`, `tee` (logging)  | `INSERT` into audit/log table                                       |
| `trap` (error handling)   | `BEGIN ... EXCEPTION WHEN ERROR THEN ... END;`                      |
| `usage()` function        | Documentation/comments, or error messages via `RAISE`               |
| `DWDate_Gib_Zeitraum`     | `CURRENT_DATE()` and `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`        |
| `pruefeParameterGesetzt`  | `ASSERT` statements for validation                                  |
| External script invocation| Nested stored procedure calls                                       |
| File I/O                  | BigQuery tables or Cloud Storage (if needed for intermediate data)  |

**Pseudocode for BigQuery Implementation:**

**Wrapper Procedure (`project.dataset.ausd_bp_ta_bpr_opt_text_wrapper`):**
```sql
CREATE OR REPLACE PROCEDURE `project.dataset.ausd_bp_ta_bpr_opt_text_wrapper`(
  IN p_stichtag STRING,
  IN p_wiederanlaufWert INT64
)
BEGIN
  DECLARE v_sysdate STRING;
  DECLARE v_stichtag STRING;
  DECLARE v_wiederanlaufWert INT64;
  DECLARE v_jobkennung STRING DEFAULT 'ausd_bp_ta_bpr_opt_text';
  DECLARE v_eintragsnr INT64;
  DECLARE v_logdatei STRING;
  DECLARE v_errnr INT64 DEFAULT 0;
  DECLARE v_errarg STRING DEFAULT '';

  BEGIN
    SET v_wiederanlaufWert = IFNULL(p_wiederanlaufWert, 0);
    SET v_sysdate = FORMAT_DATE('%d%m%Y', CURRENT_DATE());
    SET v_stichtag = IFNULL(p_stichtag, v_sysdate);

    ASSERT v_stichtag IS NOT NULL
      AS 'Stichtag must be provided or derivable';

    -- Initial log entry for job start
    INSERT INTO `project.dataset.job_log_audit`
      (job_name, status, stichtag, restart_value, created_at)
    VALUES
      (v_jobkennung, 'STARTED', v_stichtag, v_wiederanlaufWert, CURRENT_TIMESTAMP());

    -- Determine next entry number for logging
    SET v_eintragsnr = (
      SELECT IFNULL(MAX(entry_nr), 0) + 1
      FROM `project.dataset.job_log_audit`
      WHERE job_name = v_jobkennung
    );

    SET v_logdatei = CONCAT('log_', v_jobkennung, '_', CAST(v_eintragsnr AS STRING));

    -- Log entry for job running with details
    INSERT INTO `project.dataset.job_log_audit`
      (entry_nr, job_name, script_name, log_name, stichtag, status, created_at)
    VALUES
      (v_eintragsnr, v_jobkennung, 'ausd_bp_ta_bpr_opt_text_wrapper', v_logdatei, v_stichtag, 'RUNNING', CURRENT_TIMESTAMP());

    -- Call core business logic procedure
    CALL `project.dataset.k_ausd_bp_ta_bpr_opt_text`(
      v_jobkennung,
      v_stichtag,
      v_eintragsnr,
      v_wiederanlaufWert
    );

    -- Log success
    INSERT INTO `project.dataset.job_log_audit`
      (entry_nr, job_name, status, message, created_at)
    VALUES
      (v_eintragsnr, v_jobkennung, 'OK', 'Die Abarbeitung wurde ohne erkennbare Fehler beendet', CURRENT_TIMESTAMP());

  EXCEPTION WHEN ERROR THEN
    -- Log error
    INSERT INTO `project.dataset.job_log_audit`
      (entry_nr, job_name, status, message, created_at)
    VALUES
      (v_eintragsnr, v_jobkennung, 'ERROR', @@error.message, CURRENT_TIMESTAMP());
    RAISE USING MESSAGE = 'AppError: Abbruch'; -- Re-raise for external orchestration if needed
  END;
END;
```

**Core Logic Procedure (`project.dataset.k_ausd_bp_ta_bpr_opt_text`):**
```sql
CREATE OR REPLACE PROCEDURE `project.dataset.k_ausd_bp_ta_bpr_opt_text`(
  IN p_jobkennung STRING,
  IN p_stichtag STRING,
  IN p_eintragsnr INT64,
  IN p_wiederanlaufWert INT64
)
BEGIN
  -- This is where the core business logic from k_ausd_bp_ta_bpr_opt_text.ksh will be translated.
  -- Placeholder for example transformation:
  -- 1. Delete rows based on restart_value if applicable.
  -- 2. Select and insert contract cache data.

  IF p_wiederanlaufWert > 0 THEN
    DELETE FROM `project.dataset.fos_table`
    WHERE DWH_VERTRAG_ID >= p_wiederanlaufWert;
  END IF;

  INSERT INTO `project.dataset.fos_table`
  SELECT
    col1, col2, ..., DWH_VERTRAG_ID -- Actual columns from source
  FROM `project.dataset.contract_cache_source` -- Source table for contract cache
  WHERE
    gültig_von <= PARSE_DATE('%d%m%Y', p_stichtag)
    AND PARSE_DATE('%d%m%Y', p_stichtag) < gültig_bis
    AND ladedatum < PARSE_DATE('%d%m%Y', p_stichtag)
    AND (p_wiederanlaufWert = 0 OR DWH_VERTRAG_ID > p_wiederanlaufWert);

  -- Further logging or status updates can be added here.
END;
```

## 6. External Dependencies
The initial `lineage_assembled_jobs` query showed no external systems defined (`external_systems: []`). However, analysis of the script reveals dependencies that need to be addressed in BigQuery:

*   **Environment Configuration (`.dw_init`):** This file likely sets up environment variables. In BigQuery, these will be replaced by:
    *   Parameters passed to stored procedures.
    *   Configuration values stored in a BigQuery configuration table.
    *   Potentially environment variables in an orchestration layer (e.g., Cloud Composer).
*   **Helper Scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`):** These scripts provide error handling, parameter parsing utilities, and date functions.
    *   **Error Handling:** Will be replaced by BigQuery's `EXCEPTION WHEN ERROR` blocks and dedicated audit logging.
    *   **Parameter Parsing:** Directly handled by BigQuery Stored Procedure input parameters.
    *   **Date Functions:** Replaced by BigQuery's native date/time functions (`CURRENT_DATE()`, `FORMAT_DATE()`, `PARSE_DATE()`).
*   **Core Logic Script (`k_ausd_bp_ta_bpr_opt_text.ksh`):** This is a critical dependency, as it contains the main data processing logic. This will be migrated into a separate BigQuery Stored Procedure (as described in Section 3 and 5).
*   **Standard Output/Error (File Logging):** The script writes logs to a file (`$LogDatei`). This will be replaced by inserts into the `project.dataset.job_log_audit` BigQuery table.

## 7. Unresolved / Risks
*   **Missing Complexity and Automation Rate:** The `file_complexity` and `automation_rate` database tables did not contain entries for this file, making it difficult to precisely assess the migration effort and automation potential. This migration design is based purely on static code analysis and inferred patterns.
*   **Specifics of `k_ausd_bp_ta_bpr_opt_text.ksh`:** The detailed migration of the core logic inside `k_ausd_bp_ta_bpr_opt_text.ksh` is not fully elaborated here, as the source code for that script was not provided. This design assumes it primarily involves SQL-like data manipulation suitable for a BigQuery Stored Procedure. If `k_ausd_bp_ta_bpr_opt_text.ksh` contains complex file-system operations, external system calls (e.g., SFTP, external databases), or other non-SQL logic, additional design work for those components will be required, potentially involving Cloud Storage, Cloud Functions, Cloud Run, or Cloud Composer.
*   **`DWH_VERTRAG_ID` and `fos_table`:** The exact schema of `DWH_VERTRAG_ID` and the `fos_table` (FOS-Tabelle) are inferred. Accurate schema definition will be crucial during implementation.
*   **`gültig_von`, `gültig_bis`, `ladedatum`:** The format and data types of these columns in the `contract_cache_source` are assumed to be compatible with `PARSE_DATE('%d%m%Y', p_stichtag)`.

## 8. Build Plan
The migration will involve generating the following BigQuery components:

1.  **BigQuery Audit Log Table DDL:**
    *   File: `project/dataset/ddl/job_log_audit.sql`
    *   Language: BigQuery DDL
    *   Content: `CREATE TABLE IF NOT EXISTS project.dataset.job_log_audit (...)`
2.  **Wrapper Stored Procedure SQL:**
    *   File: `project/dataset/sprocs/ausd_bp_ta_bpr_opt_text_wrapper.sql`
    *   Language: BigQuery SQL (Stored Procedure)
    *   Content: The `CREATE OR REPLACE PROCEDURE ...` statement for the wrapper logic.
3.  **Core Logic Stored Procedure SQL:**
    *   File: `project/dataset/sprocs/k_ausd_bp_ta_bpr_opt_text.sql`
    *   Language: BigQuery SQL (Stored Procedure)
    *   Content: The `CREATE OR REPLACE PROCEDURE ...` statement for the core data processing.
4.  **Orchestration Definition (e.g., Cloud Composer DAG):**
    *   File: `project/dags/ausd_bp_ta_bpr_opt_text_dag.py`
    *   Language: Python
    *   Content: An Airflow DAG that calls the `project.dataset.ausd_bp_ta_bpr_opt_text_wrapper` stored procedure, handling parameter passing and scheduling.The migration design document is complete based on the provided instructions and tool outputs. I've addressed all the requested sections, integrating information from various sources and noting where data was unavailable (e.g., complexity tier, automation bucket). I also incorporated the detailed insights and pseudocode provided by the `shellscript_to_bqsql_design` CM MCP tool.