# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `r_ausd_bp_ta_bpr_apn.ksh`, an orchestration wrapper for the initial provisioning of selected base products for the BERT system. The script's responsibilities include parameter parsing, environment setup, logging, error management, and invoking a core processing script (`k_ausd_bp_ta_bpr_apn.ksh`).

The migration target platform is **Google BigQuery**. The original KornShell script has been refactored into a BigQuery Stored Procedure, leveraging BigQuery's procedural language features for parameter handling, validation, logging, and error management. The core business logic, originally in `k_ausd_bp_ta_bpr_apn.ksh`, is also migrated to a separate BigQuery Stored Procedure.

## 2. Generated artifacts

The migration process generated the following BigQuery SQL artifacts:

*   **`sql/ddl/job_audit_log.sql`**
    *   **Role:** This DDL script defines the `job_audit_log` table in BigQuery. This table serves as the centralized logging mechanism for all job executions, replacing the file-based logging of the original KornShell script. It records job status, parameters, messages, and timestamps, providing a structured and queryable audit trail.
*   **`sql/sp/k_ausd_bp_ta_bpr_apn.sql`**
    *   **Role:** This is a placeholder BigQuery Stored Procedure for the core business logic. It represents the migrated functionality of the original `k_ausd_bp_ta_bpr_apn.ksh` script. While its detailed implementation is out of scope for this specific migration document, it is designed to accept parameters from the wrapper procedure and perform the actual data processing (e.g., SELECT, INSERT, UPDATE, DELETE operations on BigQuery tables).
*   **`sql/sp/ausd_bp_ta_bpr_apn.sql`**
    *   **Role:** This BigQuery Stored Procedure is the direct migration of the `r_ausd_bp_ta_bpr_apn.ksh` wrapper script. It handles input parameter parsing (`p_stichtag`, `p_wiederanlaufWert`), applies default values, performs validation using `ASSERT` statements, manages job auditing by inserting records into `job_audit_log`, and orchestrates the execution of the `k_ausd_bp_ta_bpr_apn` kernel procedure. It also includes robust error handling using `BEGIN...EXCEPTION` blocks.

## 3. Key design decisions

The following key design decisions were made during the migration:

*   **BigQuery Stored Procedures for Orchestration and Logic:** Both the wrapper (`r_ausd_bp_ta_bpr_apn.ksh`) and the kernel (`k_ausd_bp_ta_bpr_apn.ksh`) scripts were migrated to BigQuery Stored Procedures. This centralizes the execution within BigQuery, leveraging its native capabilities for data processing and procedural logic, and reduces external dependencies.
*   **Replacement of Shell Constructs with BigQuery SQL:**
    *   **Parameter Handling:** `getopts` and shell variable assignments are replaced by `IN` parameters in the Stored Procedure definition and `DECLARE`/`SET` statements for variable manipulation.
    *   **Defaulting and Validation:** Shell `if [[ -z ... ]]` and custom validation functions are replaced by `IFNULL`, `NULLIF`, `TRIM`, and `ASSERT` statements for robust parameter handling and validation directly within SQL.
    *   **Date Handling:** Shell date commands and utility scripts are replaced by BigQuery's native date functions like `CURRENT_DATE()` and `FORMAT_DATE()`.
    *   **Logging:** `DWMSG_*` functions and file redirection are replaced by structured `INSERT` statements into a dedicated `job_audit_log` BigQuery table, enabling centralized, queryable, and persistent logging.
    *   **Error Trapping:** The `trap` mechanism is replaced by BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN...END` block, providing structured error handling and logging before re-raising exceptions.
*   **Centralized Audit Logging:** A dedicated `job_audit_log` table was introduced to capture execution details, status, and errors for all migrated jobs. This provides a consistent and queryable audit trail, improving observability and debugging capabilities compared to scattered log files.
*   **Orchestration Shift:** The original UC4 scheduler invocation will be replaced by a modern cloud-native orchestrator, such as Google Cloud Composer (Apache Airflow) or BigQuery Scheduled Queries. This aligns with cloud best practices and provides more flexible scheduling and monitoring capabilities.
*   **Modular Design:** The separation of the wrapper and kernel logic into two distinct BigQuery Stored Procedures (`ausd_bp_ta_bpr_apn` and `k_ausd_bp_ta_bpr_apn`) maintains the modularity of the original design, promoting reusability and clearer separation of concerns.

**Notable Trade-offs:**

*   **Loss of Direct File System Access:** Migrating to BigQuery Stored Procedures means losing direct access to the file system for operations like creating temporary files or reading configuration files. Any such requirements must be re-architected using BigQuery tables, Cloud Storage, or other Google Cloud services.
*   **Explicit Data Type Handling:** Shell scripts are loosely typed. BigQuery SQL requires explicit data type declarations and careful handling of conversions (e.g., `STRING` to `INT64`, `DATE` formatting).
*   **Re-implementation of Utilities:** Generic shell utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) had to be re-implemented using BigQuery's native SQL functions and procedural constructs, as they cannot be directly translated.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps are required:

1.  **BigQuery Dataset Creation:**
    *   Ensure the target BigQuery dataset (e.g., `your_project_id.your_dataset_id`) exists. If not, create it.
2.  **Deploy `job_audit_log` Table:**
    *   Execute the DDL script `sql/ddl/job_audit_log.sql` to create the audit log table in the target BigQuery dataset.
3.  **Implement and Deploy `k_ausd_bp_ta_bpr_apn` (Kernel SP):**
    *   **Crucially, the placeholder `sql/sp/k_ausd_bp_ta_bpr_apn.sql` must be fully implemented.** This involves analyzing the original `k_ausd_bp_ta_bpr_apn.ksh` script, identifying all its SQL logic, data manipulations, and dependencies, and translating them into BigQuery SQL.
    *   Deploy the completed `k_ausd_bp_ta_bpr_apn` Stored Procedure to the target BigQuery dataset.
4.  **Deploy `ausd_bp_ta_bpr_apn` (Wrapper SP):**
    *   Execute the DDL script `sql/sp/ausd_bp_ta_bpr_apn.sql` to create the wrapper Stored Procedure in the target BigQuery dataset.
5.  **Target Table Creation/Verification:**
    *   Identify all BigQuery tables that the `k_ausd_bp_ta_bpr_apn` kernel procedure will read from or write to.
    *   Ensure these tables exist in the correct dataset and have the appropriate schemas and data loaded.
6.  **IAM Permissions:**
    *   Grant the service account or user executing the BigQuery Stored Procedures the necessary IAM roles:
        *   `BigQuery Data Editor` on the target dataset (for `job_audit_log` and any tables written by the kernel SP).
        *   `BigQuery Data Viewer` on any tables read by the kernel SP.
        *   `BigQuery Job User` for executing queries and procedures.
7.  **Orchestration Setup:**
    *   **Cloud Composer (Airflow):** If using Airflow, create a new DAG that includes a `BigQueryExecuteStoredProcedureOperator` (or similar) to call `your_project_id.your_dataset_id.ausd_bp_ta_bpr_apn`. Configure the DAG to pass `p_stichtag` and `p_wiederanlaufWert` as parameters, potentially using Airflow variables or dynamic date calculations.
    *   **BigQuery Scheduled Queries:** Alternatively, set up a BigQuery Scheduled Query to execute `CALL your_project_id.your_dataset_id.ausd_bp_ta_bpr_apn('YYYYMMDD', 0);` with appropriate parameter values and scheduling frequency.
8.  **Secrets Management:**
    *   If the original `.dw_init` or other configuration files contained sensitive information (e.g., database credentials, API keys), these must be securely managed in Google Cloud Secret Manager and accessed by the orchestrator or passed as parameters.

## 5. Known gaps & unresolved references

The following items are flagged for follow-up or represent known limitations:

*   **Missing Complexity Tier:** The absence of a complexity tier for the source file means that a detailed, automated analysis of its internal complexity was not available. This might imply hidden complexities not fully captured by the current design.
*   **Kernel Script Logic (B4 Item):** The detailed migration of `k_ausd_bp_ta_bpr_apn.ksh` is a significant undertaking and is considered a separate, prerequisite task (B4 item). This includes identifying all source/target tables, complex SQL logic, and any non-SQL operations within that script.
*   **External Command Execution:** If `k_ausd_bp_ta_bpr_apn.ksh` or any of its dependencies perform non-SQL operations (e.g., file system manipulation, calls to external binaries), these will require separate solutions (e.g., Cloud Functions, Dataflow, or custom Python operators in Airflow) as they cannot be directly translated to BigQuery SQL.
*   **Environment Variables from `.dw_init`:** While the `.dw_init` sourcing is replaced, any critical configuration parameters or environment variables set by this file must be explicitly identified and integrated into the BigQuery solution (e.g., as stored procedure default values, configuration table entries, or orchestrator variables).
*   **`FOSHoleLadedatum` Function:** The commented-out reference to `FOSHoleLadedatum` suggests a specific function for retrieving a load date. If this functionality is required and was part of a utility script (e.g., `h_alis_fos_date.ksh`), its logic needs to be migrated or re-implemented if it becomes active.

## 6. Validation

Validation of the migrated job involves testing the BigQuery Stored Procedures and their orchestration.

**How to run the tests:**

1.  **Unit Test Stored Procedures:**
    *   **`k_ausd_bp_ta_bpr_apn` (Kernel SP):** Once implemented, execute this procedure directly in BigQuery with various valid and edge-case parameters. Verify that it processes data correctly and produces the expected output in target tables.
    *   **`ausd_bp_ta_bpr_apn` (Wrapper SP):**
        *   Execute the wrapper SP directly in BigQuery with:
            *   Valid `p_stichtag` (e.g., `'01012023'`) and `p_wiederanlaufWert` (e.g., `0`, `1`).
            *   Missing `p_stichtag` (should default to `CURRENT_DATE`).
            *   Empty `p_stichtag` (e.g., `''`).
            *   Invalid `p_stichtag` format (e.g., `'2023-01-01'`).
            *   `NULL` values for both parameters.
2.  **Integration Test with Orchestrator:**
    *   Trigger the job via the chosen orchestrator (Cloud Composer DAG or BigQuery Scheduled Query).
    *   Verify that the orchestrator successfully invokes the `ausd_bp_ta_bpr_apn` SP.
    *   Test with different parameter configurations as supported by the orchestrator.

**What "passing" means:**

*   **Successful Execution:** The `ausd_bp_ta_bpr_apn` Stored Procedure completes without raising an unhandled error.
*   **Audit Log Verification:**
    *   For successful runs, the `job_audit_log` table should contain a `STARTED` entry followed by an `OK` entry for the corresponding `job_kennung` and `job_nr`.
    *   For expected error scenarios (e.g., invalid `Stichtag`), the `job_audit_log` should contain a `STARTED` entry followed by an `ERROR` entry, with a descriptive `message`.
*   **Kernel SP Invocation:** The `k_ausd_bp_ta_bpr_apn` procedure is successfully called by the wrapper.
*   **Data Integrity:** Any data read, processed, or written by the `k_ausd_bp_ta_bpr_apn` procedure is accurate, complete, and matches the expected output based on the original script's logic. This includes verifying row counts, specific data values, and schema adherence in target tables.
*   **Parameter Handling:** Parameters (`stichtag`, `wiederanlaufWert`) are correctly parsed, defaulted, and passed down to the kernel procedure.
*   **Error Handling:** When an error occurs (e.g., within the kernel SP, or due to invalid input), the wrapper SP logs an `ERROR` entry to `job_audit_log` and re-raises the error, signaling failure to the orchestrator.

## 7. Rollback procedure

In case of issues with the migrated BigQuery job, the following rollback procedure can be followed to revert to the original KornShell script execution:

1.  **Disable New Orchestration:**
    *   **Cloud Composer (Airflow):** Pause or disable the Airflow DAG responsible for invoking the `ausd_bp_ta_bpr_apn` BigQuery Stored Procedure.
    *   **BigQuery Scheduled Queries:** Disable or delete the BigQuery Scheduled Query.
2.  **Re-enable Original UC4 Job:**
    *   Re-enable the original UC4 job definition: `vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_BASISPRODUKT_JP/DW.BERT_AUSD_BP_TA_BPR_APN.xml`.
    *   Ensure the UC4 agent and environment are correctly configured to execute the original `r_ausd_bp_ta_bpr_apn.ksh` script.
3.  **Verify Original Job Execution:**
    *   Monitor the re-enabled UC4 job to ensure it runs successfully and processes data as expected, producing its original log files and outputs.
4.  **Data State (if applicable):**
    *   If the migrated BigQuery job made any irreversible data changes, a data rollback strategy might be necessary. This would depend on the specific operations performed by `k_ausd_bp_ta_bpr_apn` and should be part of its detailed migration plan. For this wrapper script, direct data changes are minimal, but the kernel might.
    *   Consider BigQuery's time travel feature for recovering tables to a previous state if data corruption occurred.

This rollback procedure ensures a quick return to the previous stable state while further investigation and remediation of the migrated job can take place.