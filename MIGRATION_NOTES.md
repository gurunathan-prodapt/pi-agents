# MIGRATION_NOTES.md

## 1. Summary

This migration re-platforms the KornShell script `k_ausd_v_ta_p_discount.ksh`, which serves as a control script for a data preparation process. The original script orchestrated the execution of an underlying SQL script (`d_ausd_v_ta_p_discount.sql`), handled job parameters, implemented error logging, and managed job status.

The migration targets Google BigQuery, transforming the control logic into BigQuery Stored Procedures and the data transformation logic into BigQuery SQL. Dedicated BigQuery tables are introduced for job and error logging, replacing the shell script's file-based and implicit logging mechanisms.

## 2. Generated artifacts

The migration process generated the following BigQuery SQL files:

*   **`ddl/ta_p_discount.sql`**
    *   **Role:** Defines the Data Definition Language (DDL) for the target data table `ta_p_discount` in BigQuery. This table will store the prepared data, replacing the original target of the `d_ausd_v_ta_p_discount.sql` script.
    *   **Note:** This is a placeholder DDL and requires completion based on the actual schema derived from the source `d_ausd_v_ta_p_discount.sql`.

*   **`ddl/job_log.sql`**
    *   **Role:** Defines the DDL for the `job_log` table in BigQuery. This table centralizes the logging of job execution status, parameters, and processed record counts, replacing the implicit job status management and temporary file-based record counting of the original KornShell script.

*   **`ddl/error_log.sql`**
    *   **Role:** Defines the DDL for the `error_log` table in BigQuery. This table captures detailed error information, including error numbers, arguments, job identifiers, and timestamps, replacing the `DWMSG_MeldeFehler` function and shell-based error reporting.

*   **`procedures/d_ausd_v_ta_p_discount.sql`**
    *   **Role:** Defines a BigQuery Stored Procedure named `d_ausd_v_ta_p_discount`. This procedure is intended to encapsulate the core data transformation logic originally found in the `d_ausd_v_ta_p_discount.sql` file invoked by the KornShell script.
    *   **Note:** This is currently a placeholder and requires the actual SQL transformation logic to be extracted and implemented.

*   **`procedures/r_ausd_vertrag_control.sql`**
    *   **Role:** Defines a BigQuery Stored Procedure named `r_ausd_vertrag_control`. This procedure is the direct replacement for the `k_ausd_v_ta_p_discount.ksh` KornShell script. It handles parameter validation, orchestrates the call to `d_ausd_v_ta_p_discount` (the data transformation procedure), manages job logging (`job_log`), and implements error handling (`error_log`).

## 3. Key design decisions

The migration approach was guided by the following key design decisions:

*   **KornShell Control Script to BigQuery Stored Procedure (`r_ausd_vertrag_control`)**:
    *   **Why:** BigQuery Stored Procedures provide a native, serverless environment for orchestrating SQL logic, handling parameters, and implementing control flow directly within the data warehouse. This eliminates the need for external shell environments and their associated dependencies. It centralizes the logic closer to the data.
    *   **Trade-offs:** Requires translating shell-specific constructs (e.g., `getopts`, `if/then/else`, `exit`) into BigQuery SQL procedural language. Debugging procedural SQL can sometimes be more challenging than shell scripts.

*   **External SQL Script (`d_ausd_v_ta_p_discount.sql`) to Separate BigQuery Stored Procedure (`d_ausd_v_ta_p_discount`)**:
    *   **Why:** Maintaining the separation of concerns between control logic and data transformation logic. This promotes modularity, reusability, and easier maintenance. The control procedure can call the transformation procedure, mirroring the original shell script's behavior of invoking an external SQL file.
    *   **Trade-offs:** Requires a dedicated analysis and translation effort for the original SQL script's dialect and logic into BigQuery SQL.

*   **Dedicated BigQuery Tables for Job and Error Logging (`job_log`, `error_log`)**:
    *   **Why:** Replaces the disparate logging mechanisms (e.g., `DWMSG_MeldeFehler`, implicit job status updates, temporary files) with a centralized, structured, and queryable logging solution within BigQuery. This simplifies monitoring, auditing, and troubleshooting.
    *   **Trade-offs:** Requires defining and maintaining the schema for these new tables.

*   **Direct Procedure Arguments for Parameters**:
    *   **Why:** BigQuery Stored Procedures natively support input parameters, providing a clean and type-safe way to pass `JobKennung` and `EintragsNr` directly, replacing the shell script's command-line argument parsing (`getopts`).
    *   **Trade-offs:** None significant; this is a direct improvement.

*   **BigQuery's `SIGNAL SQLSTATE` and `EXCEPTION WHEN ERROR` for Error Handling**:
    *   **Why:** Leverages BigQuery's native error handling mechanisms for robust error management within the stored procedures. This allows for graceful termination, custom error messages, and integration with the `error_log` table.
    *   **Trade-offs:** Requires understanding and implementing BigQuery's specific error handling syntax.

*   **`SELECT COUNT(*)` for Record Counting**:
    *   **Why:** Replaces the temporary file-based mechanism for counting processed records with a direct, efficient SQL query against the target table. The result is stored in a variable and logged, streamlining the process.
    *   **Trade-offs:** The `WHERE` clause for counting records must accurately reflect the scope of records processed by the `d_ausd_v_ta_p_discount` procedure.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be performed:

1.  **BigQuery Dataset Creation:**
    *   Ensure the target BigQuery dataset (`project.dataset` in the generated code) exists. If not, create it:
        ```bash
        bq mk --dataset project:dataset
        ```
        (Replace `project` and `dataset` with your actual project ID and dataset name).

2.  **Schema Completion for `ta_p_discount`:**
    *   **Crucial Step:** The `ddl/ta_p_discount.sql` file contains a placeholder schema. Analyze the original `d_ausd_v_ta_p_discount.sql` script to determine the exact schema (column names, data types, nullability) of the `ta_p_discount` table. Update `ddl/ta_p_discount.sql` with the correct DDL.

3.  **Table Creation:**
    *   Execute the DDL scripts to create the necessary tables in your BigQuery dataset:
        ```bash
        bq query --use_legacy_sql=false < ddl/ta_p_discount.sql
        bq query --use_legacy_sql=false < ddl/job_log.sql
        bq query --use_legacy_sql=false < ddl/error_log.sql
        ```

4.  **Stored Procedure Creation:**
    *   Execute the DDL scripts to create the BigQuery Stored Procedures:
        ```bash
        bq query --use_legacy_sql=false < procedures/d_ausd_v_ta_p_discount.sql
        bq query --use_legacy_sql=false < procedures/r_ausd_vertrag_control.sql
        ```

5.  **Implement `d_ausd_v_ta_p_discount` Logic:**
    *   **Crucial Step:** The `procedures/d_ausd_v_ta_p_discount.sql` file currently contains placeholder logic. The actual data transformation logic from the original `d_ausd_v_ta_p_discount.sql` must be extracted, translated to BigQuery SQL, and implemented within this stored procedure. This may involve creating or migrating source tables that `d_ausd_v_ta_p_discount.sql` reads from.

6.  **IAM Permissions:**
    *   Ensure the Google Cloud service account or user identity that will execute `r_ausd_vertrag_control` has the following BigQuery permissions:
        *   `bigquery.datasets.get`
        *   `bigquery.tables.create`, `bigquery.tables.update`, `bigquery.tables.getData`, `bigquery.tables.insertAll` (for `ta_p_discount`, `job_log`, `error_log`)
        *   `bigquery.routines.create`, `bigquery.routines.update`, `bigquery.routines.call` (for the stored procedures)
        *   `bigquery.jobs.create` (to run queries and procedures)

7.  **Connection Strings / Configuration:**
    *   BigQuery Stored Procedures operate within a specific project and dataset. Ensure that the `project.dataset` placeholders in the generated code are updated to reflect the actual BigQuery project ID and dataset name where these objects reside. No explicit "connection string" is typically needed for procedures called within BigQuery.

8.  **Scheduling (if applicable):**
    *   If the job needs to be scheduled, configure an orchestration tool:
        *   **Cloud Composer (Apache Airflow):** Create a DAG that calls `project.dataset.r_ausd_vertrag_control` using the `BigQueryExecuteStoredProcedureOperator` or `BigQueryOperator`.
        *   **BigQuery Scheduled Queries:** Create a scheduled query that executes `CALL project.dataset.r_ausd_vertrag_control('Job123', 'Entry456');`.
        *   **Cloud Workflows/Cloud Functions:** Develop a workflow or function to trigger the BigQuery stored procedure.

## 5. Known gaps & unresolved references

The following items have been identified as gaps or require further follow-up:

*   **Full `d_ausd_v_ta_p_discount.sql` Migration (B4 Item):** The most significant gap is the actual data transformation logic from the original `d_ausd_v_ta_p_discount.sql`. This content needs to be thoroughly analyzed, translated to BigQuery SQL, and implemented within the `procedures/d_ausd_v_ta_p_discount.sql` stored procedure. This includes understanding its source tables, joins, filters, and DML operations.
*   **`ta_p_discount` Table Schema (B4 Item):** The DDL for `ddl/ta_p_discount.sql` is a placeholder. Its definitive schema must be derived from the analysis of `d_ausd_v_ta_p_discount.sql` and implemented.
*   **`starteSQLSkript` and `h_alis_sqlplus.ksh` Logic:** The original `starteSQLSkript` function (likely within `h_alis_sqlplus.ksh`) might contain complex logic related to job control, such as ignoring active jobs, updating job status tables, or specific error handling. This implicit logic needs to be fully understood and, if relevant, explicitly replicated in `r_ausd_vertrag_control` or a separate job control mechanism. The current `r_ausd_vertrag_control` only logs `STARTED` and `DONE`/`FAILED` states.
*   **`DWMSG_MeldeFehler` Exact Behavior:** While `error_log` captures basic error details, the original `DWMSG_MeldeFehler` might have had specific behaviors like sending notifications (email, pager), logging to specific system logs, or different severity levels. If these are required, additional BigQuery features (e.g., Cloud Logging integration, Pub/Sub for notifications) would be needed.
*   **Source Data Availability:** The migration assumes that the source data required by `d_ausd_v_ta_p_discount.sql` (which populates `ta_p_discount`) is already available in BigQuery or will be migrated separately. This is a prerequisite for the `d_ausd_v_ta_p_discount` procedure to function correctly.
*   **Performance Validation:** Post-migration, thorough performance testing will be required to ensure the BigQuery Stored Procedures meet or exceed the performance of the original KornShell/SQLPlus execution. This is especially critical for the `d_ausd_v_ta_p_discount` procedure.
*   **Configuration Management:** Any static values or configurations previously stored in environment variables (e.g., `$BERT_DIR_ROOT`, `$DW_DIR_UTL`) will need to be managed. For BigQuery, these could be hardcoded, passed as parameters, or stored in a BigQuery configuration table.

## 6. Validation

Validation ensures that the migrated job functions correctly and produces the expected output.

**How to run the tests:**

1.  **Prerequisites:** Ensure all manual steps (Section 4) are completed, including the full implementation of `procedures/d_ausd_v_ta_p_discount.sql` and the correct `ta_p_discount` schema.
2.  **Execute the Control Procedure:**
    *   Use the BigQuery UI, `bq` command-line tool, or an orchestration tool (e.g., Cloud Composer) to call the main control procedure:
        ```sql
        CALL `project.dataset.r_ausd_vertrag_control`('TEST_JOB_KENNUNG', 'TEST_EINTRAGS_NR');
        ```
    *   Test with various valid and invalid parameter combinations to verify validation logic.
    *   Test scenarios that would cause errors in the `d_ausd_v_ta_p_discount` procedure (e.g., missing source data, data type mismatches) to ensure error logging works.
3.  **Monitor Logs:**
    *   Check the `job_log` table for entries corresponding to your test executions.
    *   Check the `error_log` table for any errors encountered during execution.
    *   Monitor BigQuery job history for execution status and details.
4.  **Data Verification:**
    *   Query the `ta_p_discount` table to inspect the processed data.
    *   Compare the data in `ta_p_discount` with the expected output from the original system for the same input parameters. This may require running the original script and extracting its output.

**What "passing" means:**

A successful migration and validation means:

*   **Functional Equivalence:** The `r_ausd_vertrag_control` procedure executes without unhandled errors and correctly orchestrates the data transformation.
*   **Data Accuracy:** The data generated in `project.dataset.ta_p_discount` is identical (or functionally equivalent, considering BigQuery-specific data types/precision) to the data produced by the original `k_ausd_v_ta_p_discount.ksh` script and its underlying `d_ausd_v_ta_p_discount.sql`.
*   **Correct Logging:**
    *   The `job_log` table accurately reflects the start, completion, and status (`DONE` or `FAILED`) of each job execution, including the correct `records_processed` count.
    *   The `error_log` table captures all expected error conditions with relevant details (error number, argument, message, job identifiers).
*   **Parameter Handling:** The procedure correctly validates input parameters and raises appropriate errors for invalid inputs.
*   **Performance:** The execution time and resource consumption of the BigQuery procedures are within acceptable limits, ideally matching or improving upon the legacy system's performance.

## 7. Rollback procedure

In case of issues during or after go-live, the following steps outline the rollback procedure to revert to the original KornShell script:

1.  **Stop BigQuery Job Execution:**
    *   Immediately halt any scheduled executions (e.g., disable Cloud Composer DAGs, pause BigQuery Scheduled Queries) that invoke `project.dataset.r_ausd_vertrag_control`.

2.  **Revert to Original Scheduling:**
    *   Re-enable or restart the original scheduling mechanism for `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_discount.ksh`.

3.  **Data State Reversion (if necessary):**
    *   If the BigQuery job made changes to critical data that cannot be easily undone or if the data is corrupted, a data rollback strategy might be needed. This could involve:
        *   Restoring `ta_p_discount` from a backup (if backups were taken before go-live).
        *   Running a compensating transaction to revert changes.
        *   Truncating and reloading `ta_p_discount` if it's an append-only or full-refresh table.
    *   **Note:** The specific data rollback strategy depends heavily on the nature of the data transformation in `d_ausd_v_ta_p_discount` and the criticality of the `ta_p_discount` table.

4.  **Clean Up BigQuery Artifacts (Optional but Recommended):**
    *   Once the original system is confirmed to be running stably, the migrated BigQuery objects can be removed to avoid confusion and resource consumption.
    *   **Delete Stored Procedures:**
        ```sql
        DROP PROCEDURE IF EXISTS `project.dataset.r_ausd_vertrag_control`;
        DROP PROCEDURE IF EXISTS `project.dataset.d_ausd_v_ta_p_discount`;
        ```
    *   **Delete Tables:**
        ```sql
        DROP TABLE IF EXISTS `project.dataset.ta_p_discount`;
        DROP TABLE IF EXISTS `project.dataset.job_log`;
        DROP TABLE IF EXISTS `project.dataset.error_log`;
        ```
    *   **Note:** Only delete the dataset if it was created solely for this migration and contains no other critical data.