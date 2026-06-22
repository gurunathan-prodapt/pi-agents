# MIGRATION_NOTES.md: `k_ausd_v_ta_bp_ref.ksh`

## 1. Summary

The legacy KornShell script `k_ausd_v_ta_bp_ref.ksh`, which orchestrated the execution of a SQL script (`d_ausd_v_ta_bp_ref.sql`) and managed job entries, has been migrated.

The target platform is Google Cloud Platform, specifically:
*   **BigQuery Stored Procedures**: For encapsulating both the control/orchestration logic and the core business logic.
*   **BigQuery Tables**: For structured logging of errors and job execution details.
*   **Cloud Composer (Apache Airflow)**: For scheduling and triggering the BigQuery stored procedures.

The migration involved re-implementing the shell script's parameter parsing, validation, error handling, and job management logic into BigQuery SQL, and converting the core data processing SQL into a dedicated BigQuery stored procedure.

## 2. Generated Artifacts

The migration produced the following files:

*   **`bigquery/ddl/error_log.sql`**
    *   **Role**: Defines the Data Definition Language (DDL) for the `error_log` BigQuery table. This table is used to capture and store structured error messages, timestamps, error codes, and contextual information whenever an error occurs during the execution of the migrated BigQuery stored procedures. It replaces the `DWMSG_MeldeFehler` and `echo "FEHLER:..."` constructs from the original KornShell script.

*   **`bigquery/ddl/job_run_log.sql`**
    *   **Role**: Defines the DDL for the `job_run_log` BigQuery table. This table records details of each successful job execution, including input parameters, the target table name, and the number of records processed. It replaces the implicit logging and temporary file (`tmpFile`) usage for record counts in the legacy script.

*   **`bigquery/stored_procedures/d_ausd_v_ta_bp_ref_logic.sql`**
    *   **Role**: Contains the BigQuery SQL Stored Procedure `project.dataset.d_ausd_v_ta_bp_ref_logic`. This procedure encapsulates the core business logic originally found in `d_ausd_v_ta_bp_ref.sql`. It performs the actual data manipulation, including truncating and inserting data into the `project.dataset.sof_ta_bp_ref` table based on data from `project.dataset.cds_ta_bp_ref`. Error handling specific to data operations is included within this procedure.

*   **`bigquery/stored_procedures/r_ausd_vertrag_control.sql`**
    *   **Role**: Contains the BigQuery SQL Stored Procedure `project.dataset.r_ausd_vertrag_control`. This is the main control procedure, directly migrating the orchestration logic of `k_ausd_v_ta_bp_ref.ksh`. It handles:
        *   Parsing and validating input parameters (`p_job_kennung`, `p_eintrags_nr`, `p_stichtag`).
        *   Calling the `d_ausd_v_ta_bp_ref_logic` procedure to execute the core business logic.
        *   Calculating and logging the number of processed records to `job_run_log`.
        *   Logging validation and unhandled execution errors to `error_log`.
        *   Includes placeholders for "active job" management logic.

*   **`airflow/dags/k_ausd_v_ta_bp_ref_migration.py`**
    *   **Role**: An Apache Airflow DAG written in Python, designed for Cloud Composer. This DAG is responsible for scheduling and triggering the `r_ausd_vertrag_control` BigQuery stored procedure. It replaces the external scheduling mechanism of the original KornShell script, providing robust orchestration capabilities.

## 3. Key Design Decisions

1.  **Separation of Concerns (Control vs. Business Logic)**:
    *   **Decision**: The original KornShell script acted as an orchestrator, calling a separate SQL script for business logic. This pattern was preserved by creating two distinct BigQuery Stored Procedures: `r_ausd_vertrag_control` for orchestration and `d_ausd_v_ta_bp_ref_logic` for the core data transformation.
    *   **Rationale**: This modular approach enhances maintainability, testability, and reusability. It clearly separates the "how to run" from the "what to do" aspects of the job.

2.  **KornShell Scripting to BigQuery SQL Re-implementation**:
    *   **Decision**: All shell-specific constructs (e.g., `getopts` for parameter parsing, `if` conditions for validation, `echo` for logging, sourcing utility scripts) were re-implemented using native BigQuery SQL features (e.g., `IN` parameters, `IF` statements, `ASSERT`, `INSERT` into logging tables, `RAISE` for error handling).
    *   **Rationale**: This ensures the solution is fully BigQuery-native, leveraging its performance and scalability, and eliminating dependencies on external shell environments.

3.  **Structured Logging with Dedicated Tables**:
    *   **Decision**: Instead of `echo` statements and temporary files, dedicated BigQuery tables (`error_log`, `job_run_log`) were created for structured logging of job execution details and errors.
    *   **Rationale**: Structured logging provides a consistent, queryable, and auditable record of job activities, significantly improving monitoring, debugging, and operational insights compared to parsing unstructured log files.

4.  **Cloud Composer for Orchestration**:
    *   **Decision**: Cloud Composer (Airflow) was chosen as the orchestration layer to trigger the main BigQuery stored procedure.
    *   **Rationale**: Airflow provides robust scheduling, dependency management, retry mechanisms, and monitoring capabilities, which are essential for production-grade data pipelines. It replaces the implicit scheduling of the legacy system.

5.  **Direct Parameter Mapping**:
    *   **Decision**: The input parameters (`-j` for `JobKennung`, `-f` for `EintragsNr`) from the KornShell script were directly mapped to `IN` parameters of the `r_ausd_vertrag_control` BigQuery stored procedure. An additional `p_stichtag` parameter was introduced for the business logic.
    *   **Rationale**: This provides a clear and explicit interface for the BigQuery job, making it easy to understand and invoke.

6.  **Record Count Mechanism**:
    *   **Decision**: The legacy method of reading a record count from a temporary file was replaced by a `SELECT COUNT(*)` query on the target table (`project.dataset.sof_ta_bp_ref`) after the business logic execution.
    *   **Rationale**: This is a BigQuery-native and reliable way to determine the number of processed records, directly integrating with the data processing flow.

7.  **Error Handling and Propagation**:
    *   **Decision**: BigQuery's `EXCEPTION WHEN ERROR` blocks and `RAISE` statements are used to catch and propagate errors. Errors are logged to `error_log` before being re-raised.
    *   **Rationale**: This ensures that errors are captured, provide context for debugging, and are propagated up to the orchestration layer (Airflow) for proper task failure handling and alerting.

## 4. Manual Steps Before Go-Live

Before the migrated job can be run in production, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**:
    *   Ensure the BigQuery dataset `project.dataset` exists. If not, create it:
        ```sql
        CREATE SCHEMA IF NOT EXISTS `project.dataset`;
        ```
    *   **Action**: Create BigQuery Dataset.

2.  **BigQuery Table Creation (DDL Deployment)**:
    *   Deploy the DDL for the logging tables:
        *   `bigquery/ddl/error_log.sql`
        *   `bigquery/ddl/job_run_log.sql`
    *   Deploy the DDL for the source and target tables if they don't exist:
        *   **`project.dataset.cds_ta_bp_ref`**: This is a source table for `d_ausd_v_ta_bp_ref_logic`. Its schema must match the expectations of the `INSERT` statement.
        *   **`project.dataset.sof_ta_bp_ref`**: This is the target table for `d_ausd_v_ta_bp_ref_logic`. Its schema must match the `INSERT` statement (e.g., `cntrct_cp2_id`, `bp_id`).
    *   **Action**: Execute DDL scripts for `error_log`, `job_run_log`, `cds_ta_bp_ref` (if not existing), and `sof_ta_bp_ref`.

3.  **BigQuery Stored Procedure Deployment**:
    *   Deploy the BigQuery stored procedures:
        *   `bigquery/stored_procedures/d_ausd_v_ta_bp_ref_logic.sql`
        *   `bigquery/stored_procedures/r_ausd_vertrag_control.sql`
    *   **Action**: Execute `CREATE OR REPLACE PROCEDURE` statements for both stored procedures.

4.  **IAM / Permissions**:
    *   The Google Cloud service account used by Cloud Composer (or any other orchestrator) to run the BigQuery job must have the following IAM roles:
        *   `BigQuery Data Editor` on `project.dataset` (to create/truncate/insert into `sof_ta_bp_ref`, `error_log`, `job_run_log`).
        *   `BigQuery Data Viewer` on `project.dataset` (to read from `cds_ta_bp_ref` and `sof_ta_bp_ref` for counting).
        *   `BigQuery Job User` (to run BigQuery jobs, including stored procedures).
    *   **Action**: Grant necessary IAM roles to the service account.

5.  **Connection Strings / Secrets**:
    *   No explicit connection strings are required for BigQuery-native operations. All BigQuery resources are accessed via their fully qualified names (`project.dataset.table_name`).
    *   If any sensitive configuration (e.g., API keys for future extensions) were to be introduced, they should be managed via Google Secret Manager.
    *   **Action**: N/A (for this migration).

6.  **Scheduling (Cloud Composer)**:
    *   Deploy the `airflow/dags/k_ausd_v_ta_bp_ref_migration.py` DAG to your Cloud Composer environment.
    *   **Crucially**: Update the placeholder values in the DAG:
        *   `PROJECT_ID = "project"`: Replace with your actual GCP Project ID.
        *   `DATASET_ID = "dataset"`: Replace with your actual BigQuery Dataset ID.
        *   `p_job_kennung => 'YOUR_JOB_KENNUNG'`: Replace with the actual job identifier (e.g., `'BP_REF_DAILY'`).
        *   `p_eintrags_nr => 'YOUR_EINTRAGS_NR'`: Replace with the actual entry number (e.g., `'12345'`).
        *   `schedule=None`: Configure the desired Airflow schedule (e.g., `schedule="@daily"` for daily execution).
    *   **Action**: Deploy DAG and configure parameters/schedule.

## 5. Known Gaps & Unresolved References

The following items were identified as gaps or require further follow-up:

1.  **Detailed Migration of `d_ausd_v_ta_bp_ref.sql`**:
    *   The provided `d_ausd_v_ta_bp_ref_logic.sql` is a *placeholder* based on general assumptions about the original SQL script's function (truncate and insert). A thorough analysis and precise migration of the actual `d_ausd_v_ta_bp_ref.sql` content is required to ensure functional equivalence. This includes:
        *   Confirming the exact source tables and their schemas (e.g., `cds_ta_bp_ref`).
        *   Verifying all `WHERE` clause conditions and joins.
        *   Ensuring correct data type mappings and function translations (e.g., date functions).
        *   Confirming the derivation of `p_stichtag` if it was not a direct input in the original SQL.
    *   **Follow-up**: Dedicated analysis and refinement of `d_ausd_v_ta_bp_ref_logic.sql`. This is a **B4 (Redesign)** item for the core business logic.

2.  **"Active Jobs" / "Old Active Jobs" Logic**:
    *   The original KornShell script's comments indicated logic to "ignore active jobs" and "deactivate older active jobs." The migrated `r_ausd_vertrag_control` procedure includes a placeholder comment for this logic.
    *   **Resolution Required**: A clear definition of "active job" and "older active job" is needed. This will likely involve creating a dedicated BigQuery metadata table (e.g., `project.dataset.job_metadata`) to track job statuses, and implementing DML operations within `r_ausd_vertrag_control` to interact with this table.
    *   **Follow-up**: Design and implement the job metadata table and associated DML logic. This is a **B4 (Redesign)** item.

3.  **Legacy Environment Variable Resolution (`BERT_DIR_ROOT`, etc.)**:
    *   The original script relied on environment variables like `BERT_DIR_ROOT` to locate helper scripts. While these helper scripts' functionalities have been re-implemented in BigQuery SQL, the exact values or configurations they represented (e.g., base directories, specific settings) might still be relevant for other parts of the system or for understanding the full context.
    *   **Follow-up**: Confirm if any values derived from these environment variables are implicitly used elsewhere or need to be explicitly configured in the BigQuery environment (e.g., as constants in stored procedures or parameters in the Airflow DAG).

4.  **`starteSQLSkript` Internal Logic**:
    *   The exact internal workings of the `starteSQLSkript` KornShell function (e.g., how it handled SQL*Plus connections, error codes, and specifically how it populated the `tmpFile` with record counts) were not fully known. The migration assumes standard BigQuery SQL execution and `COUNT(*)` for record counting.
    *   **Follow-up**: If discrepancies arise in error handling or record counting, investigate the original `starteSQLSkript` for any unique behaviors that need to be replicated.

5.  **Missing Complexity and Automation Rate Data**:
    *   The lack of `file_complexity` and `automation_rate` data for the original script meant its migration classification (B3/B4) was an estimate. This might indicate deeper complexities not fully captured.
    *   **Follow-up**: N/A, but acknowledge the potential for unforeseen complexities.

## 6. Validation

To ensure the migrated job functions correctly and produces accurate results, follow these validation steps:

1.  **Unit Testing (BigQuery Stored Procedures)**:
    *   **`d_ausd_v_ta_bp_ref_logic`**:
        *   Create a test version of `project.dataset.cds_ta_bp_ref` with known test data.
        *   Call `CALL project.dataset.d_ausd_v_ta_bp_ref_logic('YYYY-MM-DD', 'TEST_JOB', 'TEST_ENTRY');`
        *   Verify the contents of `project.dataset.sof_ta_bp_ref` against expected output.
        *   Test with `p_stichtag` values that should include/exclude certain records.
        *   Test error handling by simulating data issues or permission errors (if possible).
    *   **`r_ausd_vertrag_control`**:
        *   Call `CALL project.dataset.r_ausd_vertrag_control('TEST_JOB', 'TEST_ENTRY', CURRENT_DATE());`
        *   Verify that `d_ausd_v_ta_bp_ref_logic` is called and completes successfully.
        *   Check `project.dataset.job_run_log` for a successful entry with the correct `records_processed` count.
        *   Test parameter validation by calling with `NULL` or empty `p_job_kennung` or `p_eintrags_nr`. Verify an entry in `project.dataset.error_log` and that the procedure raises an error.
        *   Test error propagation by introducing an error in `d_ausd_v_ta_bp_ref_logic` and verifying `r_ausd_vertrag_control` catches and logs it.

2.  **Integration Testing (Cloud Composer DAG)**:
    *   Deploy the `k_ausd_v_ta_bp_ref_migration.py` DAG to a test Cloud Composer environment.
    *   Trigger the DAG manually with test parameters (ensure `p_job_kennung`, `p_eintrags_nr` are set correctly in the DAG).
    *   Monitor the Airflow UI for successful task completion.
    *   Check BigQuery for:
        *   Successful data in `project.dataset.sof_ta_bp_ref`.
        *   Correct entries in `project.dataset.job_run_log`.
        *   Absence of errors in `project.dataset.error_log` (for successful runs).
    *   Test with parameters that should cause validation errors and verify Airflow task failure and `error_log` entries.

3.  **Data Validation**:
    *   **Record Count Comparison**: Run the legacy `k_ausd_v_ta_bp_ref.ksh` job and the migrated BigQuery job with the *same input parameters and source data*. Compare the number of records processed/inserted.
    *   **Sample Data Comparison**: Select a representative sample of records from the target table (`sof_ta_bp_ref`) generated by both the legacy and migrated jobs. Verify that the data content is identical.
    *   **Edge Case Testing**: Test with source data that includes edge cases (e.g., `NULL` values, specific date ranges, empty source tables) to ensure consistent behavior.

4.  **Performance Testing**:
    *   Compare the execution time of the migrated BigQuery job against the legacy KornShell job. BigQuery is expected to be significantly faster for data processing.

**"Passing" Criteria**:
*   All BigQuery stored procedures execute without unhandled errors.
*   The Cloud Composer DAG completes successfully for valid inputs.
*   The `project.dataset.sof_ta_bp_ref` table contains the exact same data (record count and content) as produced by the legacy job for identical source data and parameters.
*   `project.dataset.job_run_log` accurately reflects successful job runs and processed record counts.
*   `project.dataset.error_log` accurately captures and logs any expected error conditions (e.g., invalid parameters).
*   The job meets performance expectations.

## 7. Rollback Procedure

In case of critical issues or unexpected behavior after go-live, the following rollback procedure can be executed:

1.  **Disable/Delete Cloud Composer DAG**:
    *   In the Cloud Composer UI (Airflow UI), locate the `k_ausd_v_ta_bp_ref_migration` DAG.
    *   Toggle the DAG to "Off" or delete it entirely to prevent further executions of the migrated job.
    *   **Action**: Disable or delete Airflow DAG.

2.  **Revert to Legacy Execution**:
    *   Resume the execution of the original `k_ausd_v_ta_bp_ref.ksh` script using its previous scheduling mechanism.
    *   **Action**: Re-enable legacy job scheduling.

3.  **Data Recovery (if necessary)**:
    *   If the migrated job corrupted or incorrectly updated the `project.dataset.sof_ta_bp_ref` table, restore the table to a previous known good state using BigQuery's time travel capabilities or from a backup if available.
    *   **Action**: Perform BigQuery table restore if data integrity is compromised.

4.  **Clean Up (Optional)**:
    *   If the rollback is deemed permanent, the BigQuery stored procedures (`d_ausd_v_ta_bp_ref_logic`, `r_ausd_vertrag_control`) and logging tables (`error_log`, `job_run_log`) can be dropped from BigQuery.
    *   **Action**: Drop BigQuery stored procedures and logging tables.

This procedure ensures a quick return to the previous stable state while allowing for investigation of the issues encountered with the migrated solution.