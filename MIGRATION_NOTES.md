# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the legacy data warehouse job `k_ausd_bp_ta_p_basisprod.ksh` and its associated Oracle SQL script `d_ausd_bp_ta_p_basisprod.sql`. The original job, written in KornShell and Oracle SQL, was responsible for orchestrating the preparation and population of the `sof$ta_p_basisprod` table, a core "Basisprodukt" (base product) table.

The entire workflow, including both orchestration and data transformation, has been migrated to **Google Cloud's BigQuery platform**. The KornShell orchestration logic has been re-implemented using BigQuery Scripting within a BigQuery Stored Procedure, and the Oracle SQL data transformation logic has been translated into BigQuery SQL, also within a Stored Procedure.

## 2. Generated Artifacts

The migration process has resulted in the following key artifacts:

*   **`sof_ta_p_basisprod.ddl`**:
    *   **Role**: BigQuery Data Definition Language (DDL) script to create the target table `sof_ta_p_basisprod` in BigQuery. This table mirrors the structure of the original Oracle `sof$ta_p_basisprod` table.
*   **`job_audit_table.ddl`**:
    *   **Role**: BigQuery DDL script to create a dedicated audit table (`job_audit`) in BigQuery. This table is used to log the execution status, parameters, record counts, and any errors for the migrated job.
*   **`d_ausd_bp_ta_p_basisprod_sp.sql`**:
    *   **Role**: BigQuery Stored Procedure that encapsulates the core data transformation logic. This procedure translates the original `d_ausd_bp_ta_p_basisprod.sql` Oracle script, performing the `TRUNCATE` and `INSERT INTO ... SELECT ...` operations using BigQuery SQL syntax and functions.
*   **`k_ausd_bp_ta_p_basisprod_sp.sql`**:
    *   **Role**: BigQuery Stored Procedure that serves as the main orchestrator for the job. This procedure replaces the original `k_ausd_bp_ta_p_basisprod.ksh` KornShell script. It handles parameter validation, date calculations, calls the `d_ausd_bp_ta_p_basisprod_sp` for data transformation, captures record counts, and logs job status to the `job_audit` table.

## 3. Key Design Decisions

The following key design decisions were made during the migration:

*   **BigQuery Stored Procedures for Orchestration and Transformation**:
    *   **Approach**: Both the shell script's orchestration and the SQL script's data transformation logic were consolidated into BigQuery Stored Procedures. The top-level `k_ausd_bp_ta_p_basisprod_sp` handles control flow and parameter management, while `d_ausd_bp_ta_p_basisprod_sp` focuses solely on the data manipulation.
    *   **Rationale**: This approach leverages BigQuery's native capabilities for scripting and SQL execution, eliminating the need for external orchestration tools for simple job flows. It keeps the entire logic within the BigQuery ecosystem, simplifying deployment, monitoring, and maintenance.
    *   **Trade-offs**: While efficient for BigQuery-native operations, this approach might be less flexible for integrating with non-BigQuery systems compared to a more generic orchestrator like Cloud Composer. However, for this specific job, the benefits of BigQuery-native execution outweigh this.
*   **Direct Translation of Oracle SQL to BigQuery SQL**:
    *   **Approach**: Oracle-specific SQL constructs (e.g., `decode`, `(+)` for outer joins, `TO_CHAR` with specific format masks, `/*+ APPEND */` hints) were directly translated to their BigQuery equivalents (e.g., `CASE` statements, explicit `LEFT JOIN`, `FORMAT_DATE`, removal of hints).
    *   **Rationale**: This minimizes functional changes and ensures the core data transformation logic remains consistent. BigQuery's optimizer is robust and does not require manual hints like Oracle.
    *   **Trade-offs**: Requires careful review and testing to ensure semantic equivalence, especially for complex Oracle functions or implicit behaviors.
*   **Replacement of Shell Utilities with BigQuery Scripting**:
    *   **Approach**: Legacy KornShell utility scripts (e.g., `h_alis_date.ksh`, `gestern.ksh`, `f_alis_msgerr.ksh`) were replaced by BigQuery Scripting constructs, native BigQuery functions (e.g., `CURRENT_DATE()`, `DATE_SUB()`, `REGEXP_CONTAINS`), and `RAISE` statements for error handling.
    *   **Rationale**: Integrates all logic into a single BigQuery environment, reducing external dependencies and simplifying the overall architecture.
*   **Dedicated Audit Logging**:
    *   **Approach**: A `job_audit` table was introduced in BigQuery to capture job execution details, status, and errors, replacing the commented-out `FOSJobErzeugeEintrag` functionality from the legacy script.
    *   **Rationale**: Provides a centralized, queryable log for job monitoring and troubleshooting within BigQuery.
*   **`TRUNCATE TABLE` for Target Table Refresh**:
    *   **Approach**: The Oracle `TRUNCATE TABLE ... REUSE STORAGE` (via `isbert_schema.dwpa_util_skript.runstatement`) was directly mapped to BigQuery's `TRUNCATE TABLE` statement.
    *   **Rationale**: This is the most straightforward and performant way to clear and reload a table in BigQuery, matching the original job's behavior.

## 4. Manual Steps Before Go-Live

Before the migrated job can be put into production, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**:
    *   Ensure the target BigQuery datasets (e.g., `project.dataset` as referenced in the generated code) exist. If not, create them.
2.  **Source Data Ingestion**:
    *   All source Oracle tables (`sof$ta_cntrct_dist`, `sof$ta_iccid_vertrag`, `sof$ta_rn_vertrag`, `sof$ta_rn_da_vda_tk`, `sof$ta_tarifoption`, `sof$ta_apn_vertrag`, `SOF$TA_BCP_ICCID`, `SOF$TA_BCP_MSISDN`, `isbert_schema.dwtk_meldungen`) must be ingested and continuously synchronized into their corresponding BigQuery tables. This typically involves an initial full load followed by incremental updates (e.g., via Cloud Data Fusion, Data Transfer Service, or custom ETL).
3.  **Target Table DDL Deployment**:
    *   Execute `sof_ta_p_basisprod.ddl` to create the `sof_ta_p_basisprod` table in the designated BigQuery dataset.
    *   Execute `job_audit_table.ddl` to create the `job_audit` table in the designated BigQuery dataset.
4.  **Stored Procedure Deployment**:
    *   Deploy `d_ausd_bp_ta_p_basisprod_sp.sql` and `k_ausd_bp_ta_p_basisprod_sp.sql` to the target BigQuery dataset. This involves running the `CREATE OR REPLACE PROCEDURE` statements.
5.  **IAM Permissions Configuration**:
    *   Ensure the service account or user identity that will execute the `k_ausd_bp_ta_p_basisprod_sp` has the necessary BigQuery IAM roles:
        *   `BigQuery Data Editor` (for `sof_ta_p_basisprod` and `job_audit`).
        *   `BigQuery Data Viewer` (for all source tables).
        *   `BigQuery Job User` (to run BigQuery jobs/procedures).
6.  **Scheduling Configuration**:
    *   Set up a scheduling mechanism (e.g., Cloud Scheduler triggering a Cloud Function/Workflow, or Cloud Composer DAG) to execute the `k_ausd_bp_ta_p_basisprod_sp` procedure at the required frequency and with the correct parameters.
7.  **Connection Strings/Secrets (if applicable for source ingestion)**:
    *   If the source data ingestion process requires connecting to the legacy Oracle database, ensure all necessary connection strings, credentials, and network access are securely configured.

## 5. Known Gaps & Unresolved References

The following items were identified as potential gaps or require further investigation:

*   **`d_ausd_bp_ta_p_basisprod_sp.sql` Content**: The full content of the `d_ausd_bp_ta_p_basisprod_sp.sql` was not provided in the generated code. Its implementation is assumed based on the design document's transformation logic. A thorough review of its generated BigQuery SQL against the original Oracle SQL is crucial.
*   **`isbert_schema.dwpa_util_skript.runstatement` Functionality**: The design assumes `runstatement` primarily performs a `TRUNCATE TABLE`. If this Oracle stored procedure has more complex logic (e.g., dynamic SQL, logging, or conditional execution), this complexity needs to be fully understood and replicated in BigQuery.
*   **Commented-out Legacy Code**: The original KornShell and SQL scripts contain significant commented-out sections (e.g., `sed`, `sort`, `join` commands, `FOSJobDeaktivate`). While these were ignored during migration based on the assumption they are inactive, a final verification with business owners is recommended to ensure no critical, albeit dormant, logic is missed.
*   **Character Encoding**: Potential issues with special characters (e.g., German umlauts like "ä", "ö", "ü") during data ingestion from Oracle to BigQuery should be monitored and addressed if discrepancies arise.
*   **Legacy Environment Variables**: The original script relied on environment variables like `BERT_DIR_ROOT`. While these have been replaced by BigQuery-native constructs, any implicit dependencies or configurations tied to these variables in the broader legacy ecosystem should be reviewed.
*   **Missing Complexity/Automation Data**: The original `file_complexity` and `automation_rate` metrics were unavailable. This could mean the initial effort estimation might have been less precise.

## 6. Validation

Validation of the migrated job involves ensuring functional equivalence and data integrity.

**How to Run Tests:**

1.  **Unit Testing of Stored Procedures**:
    *   Execute `d_ausd_bp_ta_p_basisprod_sp` directly with sample data in source tables to verify the transformation logic.
    *   Execute `k_ausd_bp_ta_p_basisprod_sp` with various valid and invalid parameter combinations (e.g., missing parameters, incorrect date formats) to test parameter validation and error handling.
2.  **Integration Testing**:
    *   Run the `k_ausd_bp_ta_p_basisprod_sp` procedure end-to-end using the configured scheduling mechanism (e.g., manually trigger the Cloud Function/Workflow).
    *   Use a dedicated test environment with representative data that mirrors the production legacy system.
3.  **Data Comparison**:
    *   After running the BigQuery job, compare the contents of `project.dataset.sof_ta_p_basisprod` with the corresponding `sof$ta_p_basisprod` table in the legacy Oracle system for the same processing date.
    *   Perform row-count comparisons.
    *   Perform checksums or aggregate function comparisons (e.g., `SUM`, `AVG`, `MIN`, `MAX` on key numeric columns, `COUNT(DISTINCT ...)` on key categorical columns).
    *   Sample-based row-by-row comparison for a subset of data.

**What "Passing" Means:**

*   **Successful Execution**: The `k_ausd_bp_ta_p_basisprod_sp` completes without raising unhandled exceptions, and the `job_audit` table shows a `status = 'SUCCESS'`.
*   **Record Count Match**: The `record_count` logged in `job_audit` for `sof_ta_p_basisprod` matches the record count in the legacy Oracle `sof$ta_p_basisprod` table for the same run.
*   **Data Integrity**:
    *   Row counts in the target BigQuery table match the legacy Oracle table.
    *   Key data aggregates (sums, averages, distinct counts) match between BigQuery and Oracle.
    *   Sample data comparisons confirm that individual records are transformed correctly and match the legacy output.
*   **Error Handling**: Invalid parameter inputs correctly trigger `RAISE` statements and log `status = 'FAILED'` in the `job_audit` table with appropriate error messages.

## 7. Rollback Procedure

In the event of critical issues or unexpected behavior after go-live, the following rollback procedure can be initiated:

1.  **Disable BigQuery Job Schedule**: Immediately disable or pause the BigQuery scheduled job (e.g., Cloud Scheduler, Cloud Composer DAG) that triggers `k_ausd_bp_ta_p_basisprod_sp`.
2.  **Re-enable Legacy Job Schedule**: Re-enable the original `k_ausd_bp_ta_p_basisprod.ksh` job in the legacy environment.
3.  **Data Reversion (if necessary)**:
    *   Since `sof_ta_p_basisprod` is a target table that is truncated and re-inserted, if the BigQuery job produced incorrect data, the BigQuery table can be truncated or dropped.
    *   If downstream systems in BigQuery have already consumed the erroneous data, those systems must be notified, and their data may need to be reverted or corrected based on the last known good state from the legacy system.
    *   The legacy job, once re-enabled, will repopulate the Oracle `sof$ta_p_basisprod` table with correct data.
4.  **Monitor Legacy System**: Closely monitor the re-enabled legacy job to ensure it is running correctly and producing expected output.
5.  **Investigation**: Begin a thorough investigation into the root cause of the issues that necessitated the rollback. This may involve reviewing BigQuery job logs, data discrepancies, and code.