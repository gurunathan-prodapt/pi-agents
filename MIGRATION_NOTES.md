# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the `r_ausd_v_ta_vertrag_tmp.ksh` job, originally a multi-component ETL pipeline consisting of a KornShell wrapper, an intermediate KornShell script, and an Oracle SQL script. The job's primary business purpose is the reconciliation and population of contract data into the `ta_vertrag_tmp` table.

The entire execution flow, from environment setup and parameter parsing to core data transformation and logging, has been migrated from its original KornShell/Oracle environment to **Google BigQuery**. The original shell scripts and Oracle SQL have been refactored into BigQuery Stored Procedures and DDL, with logging directed to a BigQuery table.

## 2. Generated artifacts

The migration process generated the following BigQuery SQL artifacts:

*   **`bigquery-sql/my_project.my_utils_dataset.job_log.sql`**
    *   **Role:** DDL for the `job_log` table. This table serves as the central repository for all job execution metadata, status, and messages, replacing the file-based logging mechanisms of the original KornShell scripts.
*   **`bigquery-sql/my_project.my_target_dataset.ta_vertrag_tmp.sql`**
    *   **Role:** DDL for the target data table `ta_vertrag_tmp`. This defines the schema for the table where the reconciled contract data will be stored in BigQuery.
*   **`bigquery-sql/my_project.my_utils_dataset.DWMSG_ErzeugeEintrag.sql`**
    *   **Role:** Stored Procedure (SP) for creating new entries in the `job_log` table. It's a core utility for logging job lifecycle events (start, success, failure).
*   **`bigquery-sql/my_project.my_utils_dataset.DWMSG_MeldeFehler.sql`**
    *   **Role:** Stored Procedure (SP) for reporting and logging errors. It inserts a 'FAILED' entry into `job_log` and raises an error to halt execution.
*   **`bigquery-sql/my_project.my_utils_dataset.DWMSG_SetzeStatusOK.sql`**
    *   **Role:** Stored Procedure (SP) for logging successful job completion. It inserts a 'SUCCESS' entry into `job_log`.
*   **`bigquery-sql/my_project.my_utils_dataset.DWMSG_Fehlerbehandlung.sql`**
    *   **Role:** Stored Procedure (SP) for general error handling. It constructs a detailed error message and calls `DWMSG_MeldeFehler`.
*   **`bigquery-sql/my_project.my_utils_dataset.DWMSG_ErmittleNr.sql`**
    *   **Role:** User-Defined Function (UDF) for generating a numeric identifier based on the current timestamp. Its direct usage in the migrated flow is limited but provided for completeness.
*   **`bigquery-sql/my_project.my_utils_dataset.DWMSG_Logdateiname.sql`**
    *   **Role:** User-Defined Function (UDF) for constructing a log filename string. Its direct usage in the BigQuery logging table context is limited but provided for completeness.
*   **`bigquery-sql/my_project.my_utils_dataset.DWMSG_SetzeStichtagInfo.sql`**
    *   **Role:** Stored Procedure (SP) for parsing a date string (`YYYYMMDD`) into a BigQuery `DATE` type. It's used to process the `v_datum` parameter.
*   **`bigquery-sql/my_project.my_utils_dataset.DWPA_UTIL_SKRIPT_runstatement.sql`**
    *   **Role:** Stored Procedure (SP) for logging final job status messages. This replaces a specific utility call found in the original Oracle SQL.
*   **`bigquery-sql/my_project.my_target_dataset.k_ausd_v_ta_vertrag_tmp.sql`**
    *   **Role:** Stored Procedure (SP) encapsulating the core data transformation logic. This procedure performs the `TRUNCATE` and `INSERT` operations to populate `ta_vertrag_tmp`, translating the original Oracle SQL into BigQuery SQL.
*   **`bigquery-sql/my_project.my_target_dataset.r_ausd_v_ta_vertrag_tmp.sql`**
    *   **Role:** Main orchestrator Stored Procedure (SP). This replaces the original `r_ausd_v_ta_vertrag_tmp.ksh` KornShell wrapper, handling parameter input, logging, calling the core transformation SP, and managing transactions and error handling.

## 3. Key design decisions

1.  **Consolidation into BigQuery Stored Procedures:** The original multi-script architecture (KornShell wrapper -> intermediate KornShell -> Oracle SQL) has been consolidated into a single, cohesive BigQuery Stored Procedure (`r_ausd_v_ta_vertrag_tmp`) that orchestrates the entire process. This simplifies deployment, execution, and monitoring within the BigQuery environment.
2.  **Centralized Logging:** The disparate logging mechanisms of the original shell scripts and Oracle environment have been replaced by a dedicated BigQuery `job_log` table (`my_project.my_utils_dataset.job_log`) and a suite of utility stored procedures (`DWMSG_ErzeugeEintrag`, `DWMSG_MeldeFehler`, `DWMSG_SetzeStatusOK`, `DWMSG_Fehlerbehandlung`). This provides a unified and queryable log for all job executions.
3.  **Parameterization:** The `v_datum` variable, which was a critical input in the original script, is now explicitly passed as an `IN` parameter (`p_execution_date`) to the main orchestrator stored procedure (`r_ausd_v_ta_vertrag_tmp`). This enhances clarity and control over job execution.
4.  **BigQuery SQL for Data Transformation:** The core Oracle SQL logic has been directly translated into BigQuery Standard SQL within the `k_ausd_v_ta_vertrag_tmp` stored procedure. This includes:
    *   **Data Type Mapping:** Oracle data types (e.g., `NUMBER`, `VARCHAR2`, `DATE`) have been mapped to appropriate BigQuery types (`INT64`, `STRING`, `DATE`). Explicit `CAST` operations have been added where necessary to ensure type compatibility and prevent implicit conversion issues.
    *   **Date Function Translation:** Oracle-specific date functions (e.g., `TO_DATE`, implicit date arithmetic) have been replaced with BigQuery equivalents like `PARSE_DATE`, `DATE`, and `DATE_DIFF`.
    *   **`TRUNCATE` and `INSERT`:** The original pattern of clearing and repopulating the target table is maintained using BigQuery's `TRUNCATE TABLE` and `INSERT INTO` statements.
5.  **Robust Error Handling and Transactions:** The migrated BigQuery stored procedures leverage BigQuery's `BEGIN TRANSACTION...COMMIT TRANSACTION...EXCEPTION WHEN ERROR...ROLLBACK TRANSACTION` block. This ensures atomicity of the data loading process and provides a clear mechanism for capturing and logging errors, replacing the shell's `trap` and Oracle's `WHENEVER SQLERROR` constructs.
6.  **External Orchestration Readiness:** The design anticipates that the `r_ausd_v_ta_vertrag_tmp` stored procedure will be invoked by an external orchestrator (e.g., Cloud Composer, Cloud Workflows), replacing the original UC4 job scheduling. This allows for flexible scheduling and integration with other cloud services.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:**
    *   Ensure the utility dataset `my_project.my_utils_dataset` exists.
    *   Ensure the target dataset `my_project.my_target_dataset` exists.
    *   Ensure the source dataset `my_project.source_dataset` exists and contains all required source tables (e.g., `sof$ta_cntrct_crs3`, `sof$ta_bp_ref`, etc.).
2.  **IAM Permissions Configuration:**
    *   The service account or user executing the BigQuery stored procedures must have:
        *   `bigquery.dataEditor` role on `my_project.my_target_dataset` (for `ta_vertrag_tmp`).
        *   `bigquery.dataEditor` role on `my_project.my_utils_dataset` (for `job_log` and creating/executing utility SPs/UDFs).
        *   `bigquery.dataViewer` role on `my_project.source_dataset` (for reading source tables).
        *   `bigquery.routineExecutor` role on all created stored procedures and UDFs within `my_project.my_utils_dataset` and `my_project.my_target_dataset`.
3.  **Source Data Availability:**
    *   Verify that all source tables referenced in the `k_ausd_v_ta_vertrag_tmp` procedure are present and populated in `my_project.source_dataset` with up-to-date data.
4.  **Deployment of BigQuery Artifacts:**
    *   Execute the DDL scripts (`job_log.sql`, `ta_vertrag_tmp.sql`) to create the necessary tables.
    *   Execute the DDL scripts for all utility stored procedures and UDFs (`DWMSG_ErzeugeEintrag.sql`, `DWMSG_MeldeFehler.sql`, etc.) to create them in `my_project.my_utils_dataset`.
    *   Execute the DDL scripts for the core transformation and orchestrator stored procedures (`k_ausd_v_ta_vertrag_tmp.sql`, `r_ausd_v_ta_vertrag_tmp.sql`) to create them in `my_project.my_target_dataset`.
5.  **External Orchestration Setup:**
    *   Configure the chosen external orchestrator (e.g., Cloud Composer, Cloud Workflows, or a custom scheduler) to call the `my_project.my_target_dataset.r_ausd_v_ta_vertrag_tmp` stored procedure.
    *   Ensure the orchestrator passes the `p_execution_date` parameter in `YYYYMMDD` format.
    *   Set up the desired scheduling frequency and dependencies within the orchestrator.

## 5. Known gaps & unresolved references

1.  **`v_datum` Parameter Derivation:** The original KornShell script's method for determining the `v_datum` (execution date) parameter is not fully detailed. The migration assumes it will be provided as an input to the `r_ausd_v_ta_vertrag_tmp` stored procedure. If `v_datum` was derived dynamically (e.g., from system date, a control table, or a complex calculation) in the original environment, this logic needs to be replicated in the external orchestrator that calls the BigQuery job.
2.  **`DWMSG_ErmittleNr`, `DWMSG_Logdateiname` Usage:** While these utility UDFs have been migrated, their original purpose was likely related to generating unique identifiers for file-based logs. In the BigQuery table-based logging system, their direct utility might be diminished. They are included for functional completeness but may not be actively called in the main `r_ausd_v_ta_vertrag_tmp` flow.
3.  **`DWMSG_SetzeStichtagInfo` Output Parameter:** The `p_stichtag_date` output parameter of `DWMSG_SetzeStichtagInfo` is currently not utilized in the `r_ausd_v_ta_vertrag_tmp` orchestrator. If the original script used this derived date for further processing *after* the main SQL execution, that logic is not currently replicated.
4.  **`DWPA_UTIL_SKRIPT_runstatement` Job Name:** The `job_name` parameter in `DWPA_UTIL_SKRIPT_runstatement` is hardcoded to `'FINAL_STATUS_LOG'` in the generated code, as the original Oracle SQL call did not pass a job name. This means the final status log entry will not be specific to `r_ausd_v_ta_vertrag_tmp` in the `job_log` table for this particular utility call.
5.  **External Script Dependencies (`dw_init`, `h_alis_sqlplus.ksh`):** The design document mentions `dw_init` and `h_alis_sqlplus.ksh`. If these scripts contained logic beyond environment setup and SQL execution (e.g., complex file system interactions, data movement to/from other systems, or conditional logic not directly related to the SQL query), those aspects are not covered by the current BigQuery SQL migration and would need to be addressed within the external orchestration layer.

## 6. Validation

To ensure the successful migration and correct functioning of the `r_ausd_v_ta_vertrag_tmp` job, perform the following validation steps:

1.  **Execute the Job:** Trigger the `my_project.my_target_dataset.r_ausd_v_ta_vertrag_tmp` stored procedure via the configured orchestrator or manually in BigQuery, providing a valid `p_execution_date` (e.g., `'20231026'`).
2.  **Log Verification:**
    *   Query `my_project.my_utils_dataset.job_log` to confirm that entries for `r_ausd_v_ta_vertrag_tmp` are present.
    *   Verify that the final entry for the job shows `status = 'SUCCESS'` and `exit_code = 0`.
    *   Check for any error messages or unexpected entries.
3.  **Data Volume Check:**
    *   Compare the row count of `my_project.my_target_dataset.ta_vertrag_tmp` with the row count of the corresponding source table in the Oracle environment (or a known baseline). The counts should match.
4.  **Data Quality Check:**
    *   Perform spot checks on key columns in `my_project.my_target_dataset.ta_vertrag_tmp` against the source data. Focus on:
        *   Primary identifiers (`vertrag_id_carmen`).
        *   Calculated fields (`upgradeberechtigt`).
        *   `CASE` statement outputs (`vertragsstatus`, `rechnungszahlart`, `rechnungsmedium`).
        *   Date fields (`geplant_kuend`, `vertragsbeginn`).
    *   Run a `CHECKSUM` or `HASH` comparison on a subset of data if feasible, or compare aggregates (SUM, AVG, COUNT DISTINCT) for numeric/categorical columns.
5.  **Functional Logic Validation:**
    *   Specifically test the `upgradeberechtigt` logic with contracts that fall into different categories (e.g., `number_time_measurement` is NULL/0, 12 months, 24 months) and with various `commitment_reference_date` and `cntrct_start_date` values relative to `p_execution_date`.
    *   Verify the `VDA` logic for `cntrct_template_id` values.

**Passing Criteria:**

*   The `my_project.my_target_dataset.ta_vertrag_tmp` table is successfully populated.
*   The row count in `ta_vertrag_tmp` matches the expected count from the source system for the given execution date.
*   A representative sample of data in `ta_vertrag_tmp` accurately reflects the transformed data from the source.
*   The `my_project.my_utils_dataset.job_log` table contains a 'SUCCESS' entry for the `r_ausd_v_ta_vertrag_tmp` job with `exit_code = 0`.
*   No unexpected errors or warnings are observed in BigQuery job logs.

## 7. Rollback procedure

In case of critical issues identified during validation or post-go-live, follow these steps to roll back to the original system:

1.  **Halt New Execution:**
    *   Immediately pause or disable the scheduled execution of the `my_project.my_target_dataset.r_ausd_v_ta_vertrag_tmp` job in the external orchestrator (e.g., Cloud Composer, Cloud Workflows).
2.  **Revert to Original Job:**
    *   Re-enable the original `r_ausd_v_ta_vertrag_tmp.ksh` job in the UC4 scheduler (or its equivalent original scheduling system).
3.  **Data Remediation (if necessary):**
    *   If the `ta_vertrag_tmp` table in BigQuery was populated incorrectly and could impact downstream processes, consider truncating the table:
        ```sql
        TRUNCATE TABLE `my_project.my_target_dataset.ta_vertrag_tmp`;
        ```
    *   Alternatively, if historical data needs to be preserved or a specific point-in-time restore is required, leverage BigQuery's time travel capabilities or restore from a snapshot if one was configured.
4.  **Code Cleanup (optional):**
    *   If the rollback is permanent, consider dropping the newly created BigQuery stored procedures, UDFs, and tables to avoid clutter and potential confusion.
    *   **Caution:** Only perform this step if the migration is definitively abandoned or if these artifacts are no longer needed.
    ```sql
    DROP PROCEDURE IF EXISTS `my_project.my_target_dataset.r_ausd_v_ta_vertrag_tmp`;
    DROP PROCEDURE IF EXISTS `my_project.my_target_dataset.k_ausd_v_ta_vertrag_tmp`;
    -- ... (drop other utility procedures and UDFs)
    DROP TABLE IF EXISTS `my_project.my_target_dataset.ta_vertrag_tmp`;
    DROP TABLE IF EXISTS `my_project.my_utils_dataset.job_log`;
    ```