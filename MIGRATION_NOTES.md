# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `r_ausd_v_ta_p_discount.ksh` from its legacy environment to Google Cloud Platform's BigQuery. The original script served as an orchestration wrapper for a data synchronization process targeting the `ta_p_discount` table.

The migration re-implements the orchestration logic, parameter handling, and error logging using BigQuery Stored Procedures and dedicated BigQuery logging tables. The core data synchronization logic, originally residing in `k_ausd_v_ta_p_discount.ksh`, is also being migrated to a separate BigQuery Stored Procedure.

**Migrated from:** `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount.ksh` (KornShell script)
**Target Platform:** Google BigQuery (Stored Procedures and Tables)

## 2. Generated Artifacts

The migration process generated the following BigQuery SQL artifacts:

*   **`sql/ddl/dw_job_log.sql`**
    *   **Role:** Defines the Data Definition Language (DDL) for the `dw_job_log` BigQuery table. This table is used to track the execution status, metadata, and lifecycle of job runs, replacing the file-based logging of job status.
*   **`sql/ddl/dw_error_log.sql`**
    *   **Role:** Defines the DDL for the `dw_error_log` BigQuery table. This table stores detailed error information, including messages, codes, and stack traces, replacing the error handling and logging previously managed by `f_alis_msgerr.ksh` and file-based error logs.
*   **`sql/ddl/dw_job_context.sql`**
    *   **Role:** Defines the DDL for the `dw_job_context` BigQuery table. This table stores contextual information for job runs, such as the reference date (`stichtag`), which was previously managed by shell variables and utility scripts.
*   **`sql/stored_procedures/sp_k_ausd_v_ta_p_discount.sql`**
    *   **Role:** A placeholder BigQuery Stored Procedure (`project.dataset.sp_k_ausd_v_ta_p_discount`) designed to encapsulate the core data synchronization logic for the `ta_p_discount` table. This procedure will replace the functionality of the original `k_ausd_v_ta_p_discount.ksh` script. Its current implementation is a stub that logs its execution.
*   **`sql/stored_procedures/sp_bert_v_ta_p_discount.sql`**
    *   **Role:** The main BigQuery Stored Procedure (`project.dataset.sp_bert_v_ta_p_discount`) that replaces the `r_ausd_v_ta_p_discount.ksh` KornShell script. It handles parameter parsing, job initialization, logging, error handling, and orchestrates the call to `sp_k_ausd_v_ta_p_discount`.

## 3. Key Design Decisions

*   **Orchestration Re-platforming**: The KornShell orchestration logic of `r_ausd_v_ta_p_discount.ksh` has been re-platformed to a BigQuery Stored Procedure (`sp_bert_v_ta_p_discount`). This centralizes job execution within the BigQuery environment, leveraging its native capabilities for procedural logic and error handling.
*   **Core Logic Migration**: The core data synchronization logic, originally in `k_ausd_v_ta_p_discount.ksh`, is designated for migration into a separate BigQuery Stored Procedure (`sp_k_ausd_v_ta_p_discount`). This modular approach promotes reusability and clear separation of concerns.
*   **Centralized Logging in BigQuery**: The legacy file-based logging (`LogDatei`) and custom logging functions (`DWMSG_...`) have been replaced by structured logging directly into dedicated BigQuery tables (`dw_job_log`, `dw_error_log`, `dw_job_context`). This provides queryable, centralized, and scalable logging, improving observability and auditing capabilities.
*   **Parameter Handling Transformation**: The `getopts` mechanism for command-line parameter parsing in KornShell is replaced by explicit `IN` parameters in the BigQuery Stored Procedure (`p_h`, `p_s`, `p_l`). This aligns with BigQuery's procedural language paradigm.
*   **Robust Error Handling**: The `trap` mechanisms and external error scripts (`f_alis_msgerr.ksh`) are replaced by BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks. Errors are caught, logged to `dw_error_log`, and the job status in `dw_job_log` is updated to `FAILED`, providing a consistent error management framework.
*   **Environment Abstraction**: Legacy environment variables and sourced utility scripts (`.dw_init`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) are replaced by BigQuery's built-in functions, `DECLARE` variables, and direct SQL logic within the stored procedures. For more complex configurations, BigQuery configuration tables could be used.

**Notable Trade-offs:**

*   **Increased BigQuery Dependency**: The solution is now tightly coupled with the BigQuery ecosystem, potentially increasing vendor lock-in.
*   **Loss of Direct OS Interaction**: Functionalities requiring direct operating system interaction (e.g., complex file system manipulation, external command execution) are no longer directly supported. If such needs arise from the `k_ausd_v_ta_p_discount.ksh` migration, they would require re-architecting using Cloud Storage, Cloud Functions, or Dataflow.
*   **Simplified Deployment and Management**: The migration simplifies deployment and management by consolidating the solution within a single cloud platform, leveraging BigQuery's managed service capabilities.
*   **Enhanced Observability**: Centralized logging in BigQuery tables significantly improves the ability to monitor, audit, and analyze job execution and errors using standard SQL queries.

## 4. Manual Steps Before Go-Live

The following manual steps must be completed before the migrated job can be put into production:

1.  **BigQuery Dataset Creation**:
    *   Ensure the target BigQuery dataset (`project.dataset` as referenced in the DDLs and stored procedures) exists. If not, create it:
        ```sql
        CREATE SCHEMA IF NOT EXISTS `project.dataset`
        OPTIONS(
            location="<your-gcp-region>" -- e.g., "US", "EU"
        );
        ```
2.  **Deploy Logging Table DDLs**:
    *   Execute the DDL scripts to create the necessary logging tables in BigQuery:
        *   `sql/ddl/dw_job_log.sql`
        *   `sql/ddl/dw_error_log.sql`
        *   `sql/ddl/dw_job_context.sql`
    *   Example command using `bq` CLI:
        ```bash
        bq query --use_legacy_sql=false < sql/ddl/dw_job_log.sql
        bq query --use_legacy_sql=false < sql/ddl/dw_error_log.sql
        bq query --use_legacy_sql=false < sql/ddl/dw_job_context.sql
        ```
3.  **Implement Core Logic for `sp_k_ausd_v_ta_p_discount`**:
    *   **Crucial Step**: The `sql/stored_procedures/sp_k_ausd_v_ta_p_discount.sql` artifact is currently a placeholder. The actual data synchronization logic from the original `k_ausd_v_ta_p_discount.ksh` must be analyzed, translated into BigQuery SQL, and implemented within this stored procedure. This may involve creating additional tables, views, or UDFs.
4.  **Deploy Stored Procedures**:
    *   Once `sp_k_ausd_v_ta_p_discount` is fully implemented, deploy both stored procedures to BigQuery:
        *   `sql/stored_procedures/sp_k_ausd_v_ta_p_discount.sql`
        *   `sql/stored_procedures/sp_bert_v_ta_p_discount.sql`
    *   Example command using `bq` CLI:
        ```bash
        bq query --use_legacy_sql=false < sql/stored_procedures/sp_k_ausd_v_ta_p_discount.sql
        bq query --use_legacy_sql=false < sql/stored_procedures/sp_bert_v_ta_p_discount.sql
        ```
5.  **IAM Permissions**:
    *   Ensure the service account or user identity that will execute `sp_bert_v_ta_p_discount` has the necessary BigQuery permissions:
        *   `bigquery.jobs.create` (to run queries/procedures)
        *   `bigquery.tables.getData`, `bigquery.tables.updateData`, `bigquery.tables.insertData` on `dw_job_log`, `dw_error_log`, `dw_job_context` (for logging).
        *   `bigquery.routines.call` on `sp_k_ausd_v_ta_p_discount`.
        *   Appropriate permissions on the `ta_p_discount` table (and any other tables involved in `sp_k_ausd_v_ta_p_discount`) for data manipulation (e.g., `bigquery.tables.updateData`, `bigquery.tables.insertData`, `bigquery.tables.deleteData`).
6.  **Scheduling**:
    *   Configure a scheduler to invoke `project.dataset.sp_bert_v_ta_p_discount`. Options include:
        *   **BigQuery Scheduled Queries**: For simple, time-based scheduling.
        *   **Cloud Composer (Apache Airflow)**: For complex workflows, dependencies, and external system integrations.
        *   **Cloud Scheduler + Cloud Functions**: For event-driven or HTTP-triggered execution.
7.  **Secrets/Connection Strings (Conditional)**:
    *   If the implemented `sp_k_ausd_v_ta_p_discount` needs to connect to external databases or APIs, ensure any required connection strings or secrets are securely managed (e.g., using Google Secret Manager) and accessible to the BigQuery environment or any intermediary services (like Cloud Functions).

## 5. Known Gaps & Unresolved References

The following items are identified as gaps, unresolved references, or require further follow-up:

*   **Core Logic Implementation (`sp_k_ausd_v_ta_p_discount`)**: This is the most significant "B4" item. The `sp_k_ausd_v_ta_p_discount.sql` artifact is a placeholder. The complete and accurate migration of the data synchronization logic from `k_ausd_v_ta_p_discount.ksh` is pending and critical for the job's functionality. This includes understanding the exact data sources, transformation rules, and target table operations.
*   **Exact Logging Schema Confirmation**: The DDLs for `dw_job_log`, `dw_error_log`, and `dw_job_context` are based on a reasonable interpretation of the original `DWMSG` system. A detailed comparison with the original logging schema and data content is required to ensure full fidelity and prevent data loss or misinterpretation of historical logs.
*   **Parameter `p_s` and `p_l` Usage**: The original `r_ausd_v_ta_p_discount.ksh` script declares `-s` and `-l` parameters but their specific usage within `k_ausd_v_ta_p_discount.ksh` is not detailed in the provided design. Their purpose and necessity in the BigQuery migration, particularly how `sp_k_ausd_v_ta_p_discount` should utilize them, need to be confirmed.
*   **Data Synchronization Approach**: The "data synchronization process for the 'ta_p_discount' table" is a high-level description. The exact method (e.g., full load, incremental load, change data capture (CDC), upsert logic) is undefined. This will significantly impact the design and complexity of `sp_k_ausd_v_ta_p_discount`.
*   **`_dw_init` and Other Utility Script Logic**: While the *functionality* of scripts like `$HOME/.dw_init`, `h_alis_parameter.ksh`, and `h_alis_date.ksh` is conceptually replaced, any specific configuration values, complex environment setups, or unique date calculations within these scripts need to be explicitly identified and incorporated into the BigQuery environment (e.g., as constants, configuration tables, or BigQuery UDFs).
*   **`ProgVersion` Management**: The `v_prog_version` variable in `sp_bert_v_ta_p_discount` is currently hardcoded to '1.0'. A robust versioning strategy should be implemented for BigQuery stored procedures, potentially integrating with CI/CD pipelines.

## 6. Validation

To validate the successful migration and functionality of the `r_ausd_v_ta_p_discount.ksh` job, follow these steps:

### 6.1. Unit Tests

1.  **Test `sp_bert_v_ta_p_discount` (Usage Display)**:
    *   Execute the stored procedure with the help flag:
        ```sql
        CALL project.dataset.sp_bert_v_ta_p_discount(p_h => TRUE, p_s => NULL, p_l => NULL);
        ```
    *   **Passing Means**: The query should return a result set containing the usage information (e.g., `Usage_Info`, `Usage_Detail` columns) and then terminate without error.
2.  **Test `sp_bert_v_ta_p_discount` (Successful Run - with stub `sp_k_ausd_v_ta_p_discount`)**:
    *   Execute the stored procedure with typical parameters:
        ```sql
        CALL project.dataset.sp_bert_v_ta_p_discount(p_h => FALSE, p_s => 'source_val', p_l => 'en');
        ```
    *   **Passing Means**:
        *   The procedure completes without raising an exception.
        *   Query `project.dataset.dw_job_log` for the latest entry for `job_kennung = 'r_ausd_v_ta_p_discount'`. The `status` column should be 'OK', and `end_timestamp` should be populated.
        *   Query `project.dataset.dw_job_context` for the corresponding `dw_eintrags_nr`. A record should exist with the correct `stichtag` (current date).
        *   Query `project.dataset.dw_error_log`. There should be no new entries related to this job run.
3.  **Test `sp_bert_v_ta_p_discount` (Error Handling - simulate failure in `sp_k_ausd_v_ta_p_discount`)**:
    *   Temporarily modify `sp_k_ausd_v_ta_p_discount` to raise an error (e.g., `RAISE EXCEPTION 'Simulated error in core logic';`) and redeploy it.
    *   Execute `sp_bert_v_ta_p_discount` again:
        ```sql
        CALL project.dataset.sp_bert_v_ta_p_discount(p_h => FALSE, p_s => 'source_val', p_l => 'en');
        ```
    *   **Passing Means**:
        *   The `CALL` statement should terminate with an error message indicating the failure.
        *   Query `project.dataset.dw_job_log` for the latest entry. The `status` column should be 'FAILED', and `end_timestamp` should be populated.
        *   Query `project.dataset.dw_error_log` for the corresponding `dw_eintrags_nr`. A record should exist detailing the simulated error.
    *   **Important**: Revert `sp_k_ausd_v_ta_p_discount` to its stub or actual implementation after this test.
4.  **Test `sp_k_ausd_v_ta_p_discount` (Once Implemented)**:
    *   Once the actual logic for `sp_k_ausd_v_ta_p_discount` is implemented, create specific unit tests to verify its data synchronization logic, including edge cases, data transformations, and target table updates.

### 6.2. Integration Tests

1.  **End-to-End Execution**:
    *   Execute `sp_bert_v_ta_p_discount` with production-like parameters.
    *   **Passing Means**:
        *   The procedure completes successfully.
        *   All logging tables (`dw_job_log`, `dw_error_log`, `dw_job_context`) are correctly populated with accurate status and context information.
        *   The `ta_p_discount` table (or its target equivalent) contains the expected synchronized data, matching the source system or predefined test data.
2.  **Performance Testing**:
    *   Run the job under expected load conditions to ensure it meets performance SLAs. Monitor BigQuery slot consumption and execution time.

### 6.3. Data Validation

1.  **Data Comparison**:
    *   After a successful run, compare the data in the target `ta_p_discount` table with the source data (or a known good baseline). This can be done using SQL queries, data validation tools, or manual spot checks.
    *   **Passing Means**: The data in the target table is an exact match (or matches the expected transformed state) of the source data according to the defined synchronization logic.

## 7. Rollback Procedure

In the event of critical issues post-migration, the following steps outline the procedure to roll back to the original KornShell script:

1.  **Stop New Job Executions**:
    *   Immediately halt any scheduled executions of `project.dataset.sp_bert_v_ta_p_discount` in BigQuery Scheduled Queries, Cloud Composer, or any other orchestrator.
2.  **Re-enable Original Script**:
    *   Re-enable the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount.ksh` script in its legacy environment. Ensure its scheduler is reactivated and it can run successfully.
3.  **Data Rollback (Conditional)**:
    *   **Crucial Step**: If the migrated `sp_k_ausd_v_ta_p_discount` procedure modified the `ta_p_discount` table (or any other target tables), a data rollback strategy must be executed. This could involve:
        *   Restoring the affected tables from a backup taken just before the migration.
        *   Utilizing BigQuery's time travel capability to revert tables to a previous state (e.g., `SELECT * FROM project.dataset.ta_p_discount FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)`).
        *   Running a reverse synchronization or data correction script if the changes were incremental and reversible.
    *   **Note**: The orchestration wrapper (`sp_bert_v_ta_p_discount`) itself does not directly modify business data, but its invocation of `sp_k_ausd_v_ta_p_discount` does.
4.  **Revert BigQuery Objects (Optional)**:
    *   If the BigQuery stored procedures and logging tables were created solely for this migration and are not shared, they can be dropped:
        ```sql
        DROP PROCEDURE IF EXISTS project.dataset.sp_bert_v_ta_p_discount;
        DROP PROCEDURE IF EXISTS project.dataset.sp_k_ausd_v_ta_p_discount;
        DROP TABLE IF EXISTS project.dataset.dw_job_log;
        DROP TABLE IF EXISTS project.dataset.dw_error_log;
        DROP TABLE IF EXISTS project.dataset.dw_job_context;
        ```
    *   If the logging tables are shared or contain valuable historical data, they should not be dropped. Instead, ensure that the original system's logging mechanisms are fully functional.
5.  **Post-Rollback Verification**:
    *   Verify that the original `r_ausd_v_ta_p_discount.ksh` script is running as expected and that data synchronization is occurring correctly in the legacy environment.
    *   Confirm that any data rollback was successful and the `ta_p_discount` table is in a consistent state.