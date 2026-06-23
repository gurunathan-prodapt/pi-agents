# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `k_ausd_bp_ta_apn_vertrag.ksh` and its associated SQL script `d_ausd_bp_ta_apn_vertrag.sql`. The original job served as an orchestrator for a data preparation task, handling environment setup, parameter validation, date calculations, and executing the core SQL logic.

The migration targets Google BigQuery. The KornShell orchestration logic has been translated into a BigQuery Stored Procedure (`r_ausd_bp_ta_apn_vertrag_proc`), while the core SQL logic from `d_ausd_bp_ta_apn_vertrag.sql` is represented by another BigQuery Stored Procedure (`d_ausd_bp_ta_apn_vertrag_proc`). Logging and auditing functionalities are replaced by dedicated BigQuery tables (`error_log`, `job_audit`).

## 2. Generated artifacts

The migration process generated the following BigQuery SQL artifacts:

*   **`bigquery/ddl/audit_and_log_tables.sql`**
    *   **Role:** Defines the schema for two essential BigQuery tables:
        *   `project.dataset.error_log`: Captures detailed error messages, warnings, and additional context during job execution. This replaces the functionality of `f_alis_msgerr.ksh` and `DWMSG_MeldeFehler`.
        *   `project.dataset.job_audit`: Records the execution status, parameters, processed record counts, and timestamps for each job run. This replaces the implicit job tracking and temporary file usage for record counts.
*   **`bigquery/procedures/d_ausd_bp_ta_apn_vertrag_proc.sql`**
    *   **Role:** A BigQuery Stored Procedure that serves as a placeholder for the translated core data transformation logic originally found in `d_ausd_bp_ta_apn_vertrag.sql`. It accepts relevant parameters and is expected to perform the primary ETL operations. It includes basic error handling and returns the count of processed records.
*   **`bigquery/procedures/r_ausd_bp_ta_apn_vertrag_proc.sql`**
    *   **Role:** The main orchestration BigQuery Stored Procedure. This procedure directly replaces `k_ausd_bp_ta_apn_vertrag.ksh`. It handles:
        *   Parsing and validating input parameters (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag_Str`, `p_wiederanlaufWert`).
        *   Performing date validation and conversion (`p_Stichtag_Str` to `DATE`).
        *   Calculating derived dates (`v_datum_heute`, `v_datum_gestern`).
        *   Calling `project.dataset.d_ausd_bp_ta_apn_vertrag_proc` to execute the core data logic.
        *   Logging errors to `project.dataset.error_log`.
        *   Recording job execution details and status to `project.dataset.job_audit`.

## 3. Key design decisions

*   **BigQuery Stored Procedures for Orchestration:** The entire control flow, parameter handling, and error management logic of the original KornShell script (`k_ausd_bp_ta_apn_vertrag.ksh`) has been translated into a native BigQuery Stored Procedure (`r_ausd_bp_ta_apn_vertrag_proc`). This decision leverages BigQuery's native capabilities for job execution, eliminating external shell dependencies and simplifying deployment and monitoring within the GCP ecosystem.
*   **Native BigQuery SQL for Core Logic:** The external SQL script (`d_ausd_bp_ta_apn_vertrag.sql`) is designed to be fully rewritten as a BigQuery Stored Procedure (`d_ausd_bp_ta_apn_vertrag_proc`). This ensures that the core data transformation benefits from BigQuery's performance, scalability, and SQL dialect, avoiding potential compatibility issues or the need for a separate SQL execution engine.
*   **Centralized Logging and Auditing in BigQuery:** Instead of disparate shell-based logging (`f_alis_msgerr.ksh`) and temporary files for record counts, dedicated BigQuery tables (`error_log`, `job_audit`) are used. This provides a structured, queryable, and centralized repository for operational insights, error tracking, and job status, significantly improving observability.
*   **BigQuery Native Date Functions:** All date calculations and validations (e.g., deriving yesterday's date, validating `DDMMYYYY` format) previously handled by shell scripts like `gestern.ksh` and custom functions are replaced by BigQuery's robust built-in date and time functions (`CURRENT_DATE()`, `DATE_SUB()`, `SAFE.PARSE_DATE()`). This enhances reliability and reduces code complexity.
*   **Direct Parameter Passing:** The `getopts` mechanism for command-line argument parsing in the KornShell script is replaced by direct input parameters to the BigQuery Stored Procedures. This simplifies invocation, improves type safety, and aligns with BigQuery's procedural paradigm.
*   **No External Orchestration (Initial):** For this specific job, a single BigQuery Stored Procedure is deemed sufficient for orchestration. This avoids the overhead and complexity of introducing Cloud Composer or Cloud Workflows unless future requirements necessitate more complex inter-job dependencies or external system integrations.

## 4. Manual steps before go-live

Before deploying and running the migrated BigQuery procedures in a production environment, the following manual steps are required:

1.  **BigQuery Project and Dataset Setup:**
    *   Ensure that the target GCP project and BigQuery dataset (referred to as `project.dataset` in the generated code) exist. If the dataset does not exist, it must be created manually or via `bq mk --dataset project:dataset`.
2.  **IAM Permissions Configuration:**
    *   The service account or user identity that will execute these BigQuery procedures must have appropriate IAM roles. This typically includes:
        *   `BigQuery Job User` (roles/bigquery.jobUser) to run BigQuery jobs.
        *   `BigQuery Data Editor` (roles/bigquery.dataEditor) on the target dataset(s) (e.g., `project.dataset`) to create/update tables (`error_log`, `job_audit`, `poolbasisprodukt`) and execute stored procedures.
3.  **Translate `d_ausd_bp_ta_apn_vertrag.sql` Content:**
    *   **CRITICAL STEP:** The `project.dataset.d_ausd_bp_ta_apn_vertrag_proc` procedure in `bigquery/procedures/d_ausd_bp_ta_apn_vertrag_proc.sql` is currently a placeholder. Its `BEGIN ... END` block *must be replaced* with the actual, fully translated BigQuery SQL logic from the original `d_ausd_bp_ta_apn_vertrag.sql` file. This includes converting any Oracle/legacy SQL constructs to BigQuery-compatible syntax, defining the correct target table schema (`project.dataset.poolbasisprodukt`), and ensuring the `processed_rows` output parameter accurately reflects the number of records processed.
4.  **Target Table Schema Finalization:**
    *   The `project.dataset.poolbasisprodukt` table, mentioned as the target, has a placeholder schema in the generated code. Its final schema (column names, data types, partitioning, clustering) must be defined based on the actual output of the translated `d_ausd_bp_ta_apn_vertrag.sql` logic.
5.  **Scheduling Configuration:**
    *   Set up a scheduling mechanism (e.g., Cloud Scheduler, Cloud Composer, or a custom Cloud Function/Cloud Run service) to invoke `project.dataset.r_ausd_bp_ta_apn_vertrag_proc` with the required input parameters (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag_Str`, `p_wiederanlaufWert`) at the desired frequency.
6.  **Environment Variable Mapping (if applicable):**
    *   If the original `d_ausd_bp_ta_apn_vertrag.sql` or the `k_ausd_bp_ta_apn_vertrag.ksh` script implicitly relied on environment variables for paths or configurations that are not directly passed as parameters, these must be explicitly mapped to BigQuery dataset/table references or configuration values within the procedures.

## 5. Known gaps & unresolved references

*   **`d_ausd_bp_ta_apn_vertrag.sql` Content (B4 Item):** The most significant gap is the actual content of `d_ausd_bp_ta_apn_vertrag.sql`. The `d_ausd_bp_ta_apn_vertrag_proc` procedure is a placeholder and requires a complete translation of the original SQL logic into BigQuery SQL. The complexity of this translation (e.g., specific database functions, proprietary SQL constructs, DDL/DML operations) is currently unknown. This is a critical B4 item requiring detailed analysis and implementation.
*   **`starteSQLSkript` Functionality:** The precise implementation details of the `starteSQLSkript` function (from `h_alis_sqlplus.ksh`) are not fully known. While the BigQuery procedures directly execute SQL, any specific error handling, connection management, or dynamic SQL generation logic within `starteSQLSkript` needs to be reviewed and replicated if necessary.
*   **Environment Variable Mapping:** The original script relies heavily on environment variables like `$HOME`, `$BERT_DIR_ROOT`, `$DW_DIR_UTL`. The exact values and their purpose (e.g., pointing to specific directories for temporary files, configuration files, or other scripts) need to be fully understood to ensure all implicit dependencies are correctly mapped to BigQuery-native concepts (e.g., dataset/table paths, configuration parameters).
*   **Character Encoding:** The presence of German special characters in comments suggests potential character encoding considerations. While BigQuery generally handles UTF-8 well, verification is needed to ensure all data and metadata are correctly processed without corruption.
*   **Job Management System (Commented Out):** The original script contained commented-out references to `FOSJobDeaktivate` and `FOSJobErzeugeEintrag`. If these functionalities were to be reactivated, the current `job_audit` table might need to be expanded, or integration with a more comprehensive GCP-native job management and monitoring system (beyond basic auditing) would be required.

## 6. Validation

To ensure the successful migration and correct functionality of the BigQuery procedures, the following validation steps should be performed:

1.  **Deployment to Test Environment:**
    *   Deploy all generated DDL (`audit_and_log_tables.sql`) and stored procedures (`d_ausd_bp_ta_apn_vertrag_proc.sql`, `r_ausd_bp_ta_apn_vertrag_proc.sql`) to a dedicated BigQuery test project and dataset.
2.  **Test Case Execution:**
    *   **Successful Execution (Passing Scenario):**
        *   **Action:** Call `project.dataset.r_ausd_bp_ta_apn_vertrag_proc` with a set of valid input parameters (e.g., `CALL project.dataset.r_ausd_bp_ta_apn_vertrag_proc('JOB_TEST_01', 'ENTRY_001', '01012023', 'N');`).
        *   **Expected Outcome ("Passing"):**
            *   The `project.dataset.job_audit` table should contain a new entry for this run with `status = 'SUCCESS'`.
            *   The `processed_record_count` in the `job_audit` entry should accurately reflect the number of rows processed by `d_ausd_bp_ta_apn_vertrag_proc`.
            *   The `project.dataset.error_log` table should *not* contain any entries related to this specific job run.
            *   The target table (`project.dataset.poolbasisprodukt`) should contain the expected data, transformed correctly according to the logic in `d_ausd_bp_ta_apn_vertrag_proc`.
    *   **Invalid Parameter Handling:**
        *   **Action:** Call `project.dataset.r_ausd_bp_ta_apn_vertrag_proc` with:
            *   Missing required parameters (e.g., `NULL` for `p_JobKennung`).
            *   An invalid date format for `p_Stichtag_Str` (e.g., `'2023-01-01'`, `'ABC'`, `'01-JAN-2023'`).
        *   **Expected Outcome ("Passing"):**
            *   The procedure should terminate with an error.
            *   The `project.dataset.error_log` table should contain an entry with `severity = 'ERROR'` and a descriptive message indicating the parameter validation failure or invalid date format.
            *   The `project.dataset.job_audit` table should contain an entry for this run with `status = 'FAILED'`.
    *   **Error in Core Transformation Logic:**
        *   **Action:** (Requires temporary modification of `d_ausd_bp_ta_apn_vertrag_proc` for testing) Introduce a deliberate error within `d_ausd_bp_ta_apn_vertrag_proc` (e.g., reference a non-existent table, cause a division by zero). Then call `r_ausd_bp_ta_apn_vertrag_proc`.
        *   **Expected Outcome ("Passing"):**
            *   The `r_ausd_bp_ta_apn_vertrag_proc` procedure should catch the error from the called procedure and terminate.
            *   The `project.dataset.error_log` table should contain an entry with `severity = 'ERROR'` detailing the error originating from `d_ausd_bp_ta_apn_vertrag_proc`.
            *   The `project.dataset.job_audit` table should contain an entry for this run with `status = 'FAILED'`.
3.  **Data Verification:**
    *   Perform a detailed comparison of the data generated in `project.dataset.poolbasisprodukt` by the migrated BigQuery procedures against the output of the original `k_ausd_bp_ta_apn_vertrag.ksh` script, using a controlled, identical input dataset. This is crucial for functional correctness.
4.  **Performance Testing:**
    *   Monitor BigQuery job execution times, slot consumption, and byte processing for the migrated procedures. Compare these metrics against baseline performance of the original script (if available) to ensure performance parity or improvement.

## 7. Rollback procedure

In the event that the migrated BigQuery procedures encounter critical issues in production that cannot be immediately resolved, the following rollback procedure can be executed:

1.  **Stop BigQuery Job Scheduler:**
    *   Immediately disable or pause any scheduler (e.g., Cloud Scheduler job, Cloud Composer DAG, custom service) that is configured to invoke `project.dataset.r_ausd_bp_ta_apn_vertrag_proc`.
2.  **Reactivate Original Legacy Script:**
    *   Re-enable and restart the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_vertrag.ksh` script in its legacy execution environment. Ensure it has access to its original dependencies and data sources.
3.  **Revert Data (if necessary):**
    *   If the BigQuery procedures modified any production data tables that are also consumed by other systems, a data rollback strategy might be required. This could involve:
        *   Restoring the affected BigQuery tables to a previous state using BigQuery's time travel feature (`FOR SYSTEM_TIME AS OF`).
        *   Loading a backup of the affected tables.
        *   Executing specific DML statements to undo changes.
    *   *Note:* This step is highly dependent on the impact of the BigQuery procedures on production data and should be part of a broader data recovery plan.
4.  **Drop BigQuery Objects (Optional, for clean up):**
    *   Once the legacy system is confirmed to be fully operational and stable, the migrated BigQuery objects can be dropped from the production environment to avoid confusion and resource consumption.
    *   ```sql
        DROP PROCEDURE IF EXISTS project.dataset.r_ausd_bp_ta_apn_vertrag_proc;
        DROP PROCEDURE IF EXISTS project.dataset.d_ausd_bp_ta_apn_vertrag_proc;
        DROP TABLE IF EXISTS project.dataset.error_log;
        DROP TABLE IF EXISTS project.dataset.job_audit;
        DROP TABLE IF EXISTS project.dataset.poolbasisprodukt; -- Only if this table was solely created by the migration
        ```
    *   Consider retaining `error_log` and `job_audit` for post-mortem analysis if they contain valuable information from the failed migration attempt.