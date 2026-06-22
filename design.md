# Migration Design — vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_assign.ksh

## 1. Purpose & Scope
This document outlines the migration plan for the KornShell script `r_ausd_v_ta_inv_assign.ksh` to Google BigQuery.

The original script (labeled "shell" and "KornShell") serves as a wrapper or orchestration script for the contract data reconciliation job for the `ta_inv_assign` table. Its primary functions include:
- Initializing the runtime environment by sourcing common utility scripts (`. $HOME/.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`).
- Parsing and validating command-line parameters (though `-s` and `-l` are parsed but not explicitly used in the provided code snippet).
- Setting up a robust logging and error handling framework using `DWMSG_*` functions and shell `trap` commands.
- Invoking the core processing script, `${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_inv_assign.ksh`, passing job-specific identifiers.
- Recording the job's start, success, or failure status.

The scope of this migration focuses on translating the wrapper script's orchestration, parameter handling, and logging mechanisms to BigQuery stored procedures and associated BigQuery tables. The actual business transformation logic within the core script `k_ausd_v_ta_inv_assign.ksh` is assumed to be migrated separately into BigQuery SQL or other BigQuery-compatible processing.

## 2. Source Inventory
The job consists of a single primary source file, with several implicit dependencies.

*   **File:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_assign.ksh`
    *   **Technology:** Shell (KornShell)
    *   **Tier:** Medium
    *   **Automation Bucket:** semi_auto
    *   **Summary:** This is a framework script for the reconciliation of contract data for the ta_inv_assign table, acting as a wrapper for a core processing script.
    *   **Purpose:** etl (orchestration/wrapper)

## 3. Target Architecture
The migrated solution will primarily leverage Google Cloud Platform (GCP) services, with BigQuery as the central data warehouse and processing engine.

*   **BigQuery Stored Procedure:** A BigQuery SQL stored procedure will encapsulate the wrapper logic of `r_ausd_v_ta_inv_assign.ksh`. This procedure will handle parameter parsing, job metadata initialization, and invocation of the core logic (which will be another BigQuery stored procedure).
    *   **Proposed Name:** `project.dataset.vertragsdatenabgleich_wrapper`
*   **BigQuery Tables for Logging/Auditing:** Dedicated BigQuery tables will replace the file-based logging and status tracking.
    *   `project.dataset.dw_job_entries`: To store job metadata, start/end times, and final status (replaces `DWMSG_SetzeStatusOK`, etc.).
    *   `project.dataset.dw_job_audit`: To store detailed log messages (replaces `LogDatei` content and `DWMSG_ErzeugeEintrag`).
    *   `project.dataset.dw_error_log`: To store specific error details (replaces `DWMSG_MeldeFehler` output).
*   **BigQuery Stored Procedure for Core Logic:** The `k_ausd_v_ta_inv_assign.ksh` core script's logic will be migrated into a separate BigQuery stored procedure.
    *   **Proposed Name:** `project.dataset.k_ausd_v_ta_inv_assign`
*   **Orchestration (Optional/External):** Cloud Composer (Airflow), Cloud Workflows, or Cloud Run could be used to trigger the main BigQuery stored procedure and manage its execution, especially if external dependencies or non-BigQuery steps are introduced by the core script.

## 4. Data Flow & Lineage
The original script does not perform direct data reads or writes but orchestrates the execution of a core script that would handle data interactions.

**Legacy Flow:**
1.  **`r_ausd_v_ta_inv_assign.ksh` (Wrapper):**
    *   Initializes environment and utility functions.
    *   Parses command-line arguments.
    *   Sets up job logging and error handling.
    *   Generates `JobKennung`, `DW_EintragsNr`, `LogDatei`.
    *   Invokes **`${BERT_DIR_ROOT}/aufbereitung/bin/k_ausd_v_ta_inv_assign.ksh` (Core Script)** with `-j $JobKennung -f ${DW_EintragsNr}`.
    *   Captures stdout/stderr from core script into `LogDatei`.
    *   Updates job status (`DWMSG_SetzeStatusOK`) based on core script's exit.

**Target Flow (BigQuery):**
1.  **`project.dataset.vertragsdatenabgleich_wrapper` (BigQuery Stored Procedure):**
    *   Receives parameters (e.g., `p_h`, `p_s`, `p_l`).
    *   Initializes internal variables (`ProgName`, `JobKennung`, `v_sysdate`).
    *   Records job start in `project.dataset.dw_job_entries`.
    *   Records audit messages in `project.dataset.dw_job_audit`.
    *   Calls **`project.dataset.k_ausd_v_ta_inv_assign` (BigQuery Stored Procedure)**, passing `JobKennung` and `DW_EintragsNr`.
    *   Handles errors using BigQuery's `EXCEPTION WHEN ERROR` block, recording them in `project.dataset.dw_error_log` and `project.dataset.dw_job_audit`.
    *   Updates final job status in `project.dataset.dw_job_entries`.

**Lineage:**
- No direct `READS` or `WRITES` edges were found for `r_ausd_v_ta_inv_assign.ksh` itself. The primary interaction is `INVOKES` the core script.
- The wrapper script relies on a number of framework scripts (e.g., `f_alis_msgerr.ksh`) and configuration files (`.dw_init`) which should be represented by BigQuery configurations or dedicated utility procedures.

## 5. Transformation Logic
The transformation logic for the wrapper script primarily involves converting shell scripting constructs into BigQuery SQL procedural language.

*   **Parameter Parsing (`getopts`):** Replaced by BigQuery stored procedure input parameters. Validation logic will use `IF` statements.
*   **Environment Sourcing (`. $HOME/.dw_init`):** Replaced by configurable procedure parameters, BigQuery session variables, or values retrieved from a BigQuery configuration table.
*   **Variable Declarations (`ProgName="...", typeset -u JobKennung="..."`):** Mapped to `DECLARE` statements in BigQuery SQL. Date functions (`date +%d%m%Y`) will use `FORMAT_DATE(..., CURRENT_DATE())`.
*   **Logging (`DWMSG_...`, `print`, `tee -a $LogDatei`):** Replaced by `INSERT` statements into BigQuery logging tables (`dw_job_entries`, `dw_job_audit`, `dw_error_log`).
*   **Error Handling (`set -eu`, `trap`, `if [ ! $ErrNr -eq 0 ]`):** Translated to BigQuery's `BEGIN ... EXCEPTION WHEN ERROR THEN ... END` block for runtime errors, and `IF` conditions for parameter validation. `SIGNAL SQLSTATE` will be used for explicit error signaling.
*   **Core Script Invocation (`${Name_Kernskript} ...`):** Replaced by a `CALL` statement to the `project.dataset.k_ausd_v_ta_inv_assign` BigQuery stored procedure.

**Example BQ SQL Pseudocode:**
```sql
CREATE OR REPLACE PROCEDURE `project.dataset.vertragsdatenabgleich_wrapper`(
  IN p_h STRING,
  IN p_s STRING,
  IN p_l STRING
)
BEGIN
  DECLARE ProgName STRING DEFAULT 'Vertragsdatenabgleich';
  DECLARE JobKennung STRING DEFAULT 'BERT_V_TA_INV_ASSIGN';
  DECLARE v_sysdate STRING DEFAULT FORMAT_DATE('%d%m%Y', CURRENT_DATE());
  DECLARE DW_EintragsNr INT64;

  -- Parameter validation (replaces getopts and if [ ! $ErrNr -eq 0 ])
  IF p_h IS NOT NULL AND p_h = 'h' THEN
    -- Print usage and exit (SELECT statement for output)
    RETURN;
  END IF;

  -- Job setup (replaces DWMSG_ErmittleNr, DWMSG_Logdateiname, DWMSG_ErzeugeEintrag)
  SET DW_EintragsNr = (SELECT IFNULL(MAX(entry_nr), 0) + 1 FROM `project.dataset.dw_job_entries` WHERE job_kennung = JobKennung);
  -- Logdatei generation is implicitly handled by the audit table entries

  INSERT INTO `project.dataset.dw_job_entries`
    (entry_nr, job_kennung, script_name, sysdate_ddmmyyyy, status, created_at)
  VALUES
    (DW_EintragsNr, JobKennung, 'vertragsdatenabgleich_wrapper', v_sysdate, 'STARTED', CURRENT_TIMESTAMP());

  INSERT INTO `project.dataset.dw_job_audit`
    (entry_nr, job_kennung, message, created_at)
  VALUES
    (DW_EintragsNr, JobKennung, 'Job gestartet', CURRENT_TIMESTAMP());

  BEGIN
    -- Core execution (replaces ${Name_Kernskript} call)
    CALL `project.dataset.k_ausd_v_ta_inv_assign`(JobKennung, DW_EintragsNr);

    -- Completion (replaces success message and DWMSG_SetzeStatusOK)
    INSERT INTO `project.dataset.dw_job_audit`
      (entry_nr, job_kennung, message, created_at)
    VALUES
      (DW_EintragsNr, JobKennung, 'Die Abarbeitung wurde ohne erkennbare Fehler beendet', CURRENT_TIMESTAMP());

    UPDATE `project.dataset.dw_job_entries`
    SET status = 'OK', finished_at = CURRENT_TIMESTAMP()
    WHERE entry_nr = DW_EintragsNr AND job_kennung = JobKennung;

  EXCEPTION WHEN ERROR THEN
    -- Error handling (replaces trap ERR and DWMSG_Fehlerbehandlung)
    INSERT INTO `project.dataset.dw_job_audit`
      (entry_nr, job_kennung, message, created_at)
    VALUES
      (DW_EintragsNr, JobKennung, 'AppError: Abbruch', CURRENT_TIMESTAMP());

    UPDATE `project.dataset.dw_job_entries`
    SET status = 'ERROR', finished_at = CURRENT_TIMESTAMP()
    WHERE entry_nr = DW_EintragsNr AND job_kennung = JobKennung;

    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'AppError: Abbruch';
  END;
END;
```

## 6. External Dependencies
The `lineage_assembled_jobs` query showed no explicit `external_systems` for this job. However, the script itself indicates several implicit dependencies.

*   **Shell Environment Initialization (`. $HOME/.dw_init`):** This local environment setup will be replaced by BigQuery procedure parameters, BigQuery session variables, or managed configuration in a GCP environment (e.g., secret manager for sensitive values, config files deployed with Cloud Functions/Run/Composer).
*   **Utility Scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`):** These framework scripts encapsulate common functions. Their logic will need to be re-implemented as BigQuery helper functions or smaller stored procedures, or their functionality absorbed directly into the wrapper and core procedures.
*   **Core Processing Script (`k_ausd_v_ta_inv_assign.ksh`):** This is the most significant dependency. It is assumed to contain the business logic for data reconciliation. This script *must* be migrated to a BigQuery stored procedure or a different BigQuery-compatible processing framework (e.g., Dataflow, Dataproc, Python with BigQuery client library). This migration is beyond the scope of this document but is a critical prerequisite.

## 7. Unresolved / Risks
*   **Core Script (`k_ausd_v_ta_inv_assign.ksh`) Logic:** The content and complexity of the core script are unknown. Its migration is essential and will likely determine the overall complexity and timeline of the full job migration. This is the primary unresolved item.
*   **`DWMSG_*` Function Details:** The exact implementation of the `DWMSG_*` functions is not known. While general logging and error handling have been proposed, specific nuances (e.g., how `DWMSG_ErmittleNr` generates numbers, specific log formats) will need to be analyzed from the source of those utility scripts.
*   **Unused Parameters (`-s`, `-l`):** The script parses these parameters but doesn't use them. If these were intended for future functionality or are used by the core script, this must be clarified.
*   **Error Numbering (`ErrNr`):** The specific error numbers (e.g., `192`, `193`) and their meanings are tied to the legacy system's error concept. These will need to be mapped to a BigQuery-compatible error management strategy, potentially using custom SQLSTATEs or a dedicated error lookup table.
*   **Resource Management:** The shell script implicitly manages resources through its execution environment. In BigQuery, resource allocation and query optimization are handled by the service, but the overall efficiency of the migrated solution will depend on the BigQuery SQL quality.
*   **Absence of Lineage Edges:** The lack of discovered lineage edges for this file might indicate a limitation in the static analysis or that the script's interactions are primarily through external invocations that are harder to trace without dynamic analysis.

## 8. Build Plan
The build plan focuses on the creation of BigQuery objects to replace the wrapper script's functionality.

1.  **Define BigQuery Datasets:**
    *   Create `project.dataset` (if it doesn't exist) to house the stored procedures and logging tables.
    *   **Language:** DDL (SQL)

2.  **Create Logging and Audit Tables:**
    *   `project.dataset.dw_job_entries`
    *   `project.dataset.dw_job_audit`
    *   `project.dataset.dw_error_log`
    *   **Language:** DDL (SQL)

3.  **Develop BigQuery Stored Procedure for Core Logic:**
    *   **FILE:** `k_ausd_v_ta_inv_assign.sql` (placeholder - *requires analysis and migration of the actual core script*).
    *   **Language:** BigQuery SQL

4.  **Develop BigQuery Stored Procedure for Wrapper Logic:**
    *   **FILE:** `vertragsdatenabgleich_wrapper.sql`
    *   Translate the KornShell script `r_ausd_v_ta_inv_assign.ksh` into BigQuery SQL, implementing parameter parsing, job entry, audit logging, error handling, and calling the `k_ausd_v_ta_inv_assign` procedure.
    *   **Language:** BigQuery SQL

5.  **Implement Utility Functions/Procedures (if necessary):**
    *   Based on the analysis of `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, create corresponding BigQuery UDFs or small stored procedures.
    *   **Language:** BigQuery SQL

6.  **Develop Orchestration Mechanism (Optional):**
    *   If external triggers or complex scheduling are required, create a Cloud Composer DAG, Cloud Workflow, or Cloud Run service to invoke the `vertragsdatenabgleich_wrapper` stored procedure.
    *   **Language:** Python (for Airflow/Workflows), or relevant language for Cloud Run.

7.  **Configuration Management:**
    *   Define and manage configuration parameters (e.g., `BERT_DIR_ROOT` values) using appropriate GCP services (Secret Manager, ConfigMap in GKE if using containers, or simple parameter tables in BigQuery).
    *   **Language:** YAML/JSON/SQL

8.  **Testing and Validation:**
    *   Develop unit and integration tests for all BigQuery procedures and logging mechanisms.
    *   Validate end-to-end functionality, ensuring correct parameter handling, logging, error propagation, and invocation of the core logic.
    *   **Language:** BigQuery SQL (for stored procedure testing), Python (for orchestration testing).