# MIGRATION_NOTES.md

## 1. Summary

The KornShell orchestration script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_instance.ksh` has been migrated to Google BigQuery.

The original script's responsibilities included:
*   Parsing and validating input parameters (Job ID, Entry Number, Reference Date, Restart Value).
*   Setting up the execution environment by sourcing helper scripts.
*   Executing a core SQL script (`d_ausd_bp_ta_bpr_instance.sql`) with the gathered parameters.
*   Handling basic error reporting.
*   Recording processed record counts.

The target platform is Google BigQuery, where the orchestration logic is now encapsulated within a BigQuery Stored Procedure (`project.dataset.r_ausd_bp_ta_bpr_instance`). The core SQL logic, originally in `d_ausd_bp_ta_bpr_instance.sql`, is intended to be migrated into a separate BigQuery Stored Procedure (`project.dataset.d_ausd_bp_ta_bpr_instance`). Audit and error logging are handled by dedicated BigQuery tables.

## 2. Generated artifacts

The migration process generated the following BigQuery DDL and Stored Procedure definitions:

*   **`my_dataset/ddl/error_log.sql`**
    *   **Role:** Defines the DDL for the `project.dataset.error_log` table. This table centralizes error messages and details, replacing the shell script's `f_alis_msgerr.ksh` and ad-hoc error handling.
*   **`my_dataset/ddl/job_audit.sql`**
    *   **Role:** Defines the DDL for the `project.dataset.job_audit` table. This table stores execution metrics, such as record counts, providing a persistent and queryable audit trail, replacing the temporary file-based record count (`$DW_DIR_UTL/bert_k_ausd_bp_ta_bpr_instance.tmp`).
*   **`my_dataset/ddl/job_control.sql`**
    *   **Role:** Defines the DDL for the `project.dataset.job_control` table. This is an optional table, intended for use if the commented-out job management logic (e.g., `FOSJobDeaktivate`, `FOSJobErzeugeEintrag`) from the original KornShell script needs to be reactivated and managed within BigQuery.
*   **`my_dataset/procedures/d_ausd_bp_ta_bpr_instance.sql`**
    *   **Role:** This is a placeholder BigQuery Stored Procedure (`project.dataset.d_ausd_bp_ta_bpr_instance`) for the core data manipulation logic. The actual SQL content from the original `d_ausd_bp_ta_bpr_instance.sql` needs to be translated and inserted into this procedure. It will perform the data reads, transformations, and writes.
*   **`my_dataset/procedures/r_ausd_bp_ta_bpr_instance.sql`**
    *   **Role:** Defines the main BigQuery Stored Procedure (`project.dataset.r_ausd_bp_ta_bpr_instance`). This procedure orchestrates the entire process, including parameter parsing and validation, date derivation, calling the core SQL logic (via `d_ausd_bp_ta_bpr_instance`), and logging audit information. It replaces the `k_ausd_bp_ta_bpr_instance.ksh` script.

## 3. Key design decisions

*   **Orchestration within BigQuery Stored Procedures**: The entire orchestration logic, including parameter handling, validation, and flow control, has been moved into a BigQuery Stored Procedure (`r_ausd_bp_ta_bpr_instance`). This eliminates the dependency on KornShell, external helper scripts, and the underlying operating system, leveraging BigQuery's native scripting capabilities for a more integrated and cloud-native solution.
*   **Separation of Orchestration and Core Logic**: The orchestration (`r_ausd_bp_ta_bpr_instance`) and the core data manipulation logic (`d_ausd_bp_ta_bpr_instance`) are implemented as separate BigQuery Stored Procedures. This promotes modularity, reusability of the core logic, and easier maintenance, as the data transformation can be updated independently of the orchestration wrapper.
*   **Centralized Audit and Error Logging**: Dedicated BigQuery tables (`error_log`, `job_audit`) are used for persistent and queryable logging of execution status, errors, and metrics. This replaces disparate file-based logging and temporary files, providing better visibility and easier debugging.
*   **Native BigQuery Features for Utilities**: Shell script utilities for date derivation (`gestern.ksh`) and parameter validation (`h_alis_date.ksh`, `h_alis_parameter.ksh`) are replaced by native BigQuery SQL functions (e.g., `CURRENT_DATE()`, `DATE_SUB()`, `PARSE_DATE()`, `IF` conditions, `RAISE`). This simplifies the code and removes external dependencies.
*   **Direct Parameter Passing**: Input parameters are passed directly as arguments to the BigQuery Stored Procedure, ensuring type safety and clear interface definition, replacing the `getopts` mechanism.

**Notable Trade-offs:**

*   **Dependency on `d_ausd_bp_ta_bpr_instance.sql` migration**: The success of this migration heavily relies on the accurate and complete translation of the core `d_ausd_bp_ta_bpr_instance.sql` script into BigQuery SQL. This is the most complex part of the data transformation.
*   **Loss of direct file system interaction**: Any logic involving direct file manipulation (e.g., `sed`, `sort`, `join` on files, if the commented-out sections were to be reactivated) will need to be re-implemented using BigQuery SQL (e.g., string functions, array operations, `UNNEST`) or potentially Cloud Dataflow for large-scale file processing.
*   **External Orchestration for Scheduling**: While BigQuery supports scheduled queries, complex dependencies or external triggers might still require an external orchestrator like Airflow, which would then call the BigQuery Stored Procedure.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**:
    *   Ensure the target BigQuery dataset (`project.dataset` as referenced in the generated code) exists in your GCP project. If not, create it.
2.  **Deploy DDLs for Audit/Error Tables**:
    *   Execute the DDL statements from `my_dataset/ddl/error_log.sql`, `my_dataset/ddl/job_audit.sql`, and `my_dataset/ddl/job_control.sql` (if `job_control` is needed) in BigQuery to create these tables.
3.  **Migrate and Deploy Core SQL Logic**:
    *   **Crucial Step**: The content of the original `d_ausd_bp_ta_bpr_instance.sql` must be fully translated into BigQuery-compatible SQL.
    *   Edit `my_dataset/procedures/d_ausd_bp_ta_bpr_instance.sql` and replace the placeholder comments with the actual migrated SQL logic.
    *   Deploy this updated stored procedure to BigQuery.
4.  **Deploy Orchestration Stored Procedure**:
    *   Deploy `my_dataset/procedures/r_ausd_bp_ta_bpr_instance.sql` to BigQuery.
    *   **Update Placeholders**: Ensure `project.dataset.target_bpr_instance_table` and `processing_date_col` within `r_ausd_bp_ta_bpr_instance.sql` are updated to reflect the actual target table and its date column name used by `d_ausd_bp_ta_bpr_instance`.
5.  **Target Table DDL**:
    *   Ensure the DDL for the actual target table (e.g., `project.dataset.target_bpr_instance_table`) that `d_ausd_bp_ta_bpr_instance` writes to is created and deployed. This DDL is not part of the generated artifacts.
6.  **IAM Permissions**:
    *   The service account or user executing the BigQuery stored procedures must have appropriate IAM roles:
        *   `BigQuery Data Editor` on the target dataset(s) to create/update tables and insert data into `error_log`, `job_audit`, `job_control`, and the main target tables.
        *   `BigQuery Job User` to run BigQuery jobs (including stored procedures).
7.  **Scheduling (if applicable)**:
    *   If using an external orchestrator (e.g., Airflow), create and configure the DAG to call `project.dataset.r_ausd_bp_ta_bpr_instance` with the required parameters.
    *   If using BigQuery Scheduled Queries, configure a new scheduled query to execute the stored procedure.
8.  **Connection Strings/Secrets**:
    *   For external orchestrators, ensure BigQuery connection details are securely configured (e.g., service account key file path or workload identity federation).

## 5. Known gaps & unresolved references

The following items are known gaps or require further attention:

*   **Core SQL Logic (`d_ausd_bp_ta_bpr_instance.sql` migration)**: The most significant gap is the actual content of the `project.dataset.d_ausd_bp_ta_bpr_instance` stored procedure. It currently contains only a placeholder. The full translation of the original Oracle/legacy SQL script into BigQuery SQL is a critical, separate task that must be completed. This includes identifying source and target tables, translating SQL syntax, and optimizing for BigQuery.
*   **Source and Target Table DDLs**: The DDLs for the source tables read by `d_ausd_bp_ta_bpr_instance` and the final target table(s) it writes to (e.g., `project.dataset.target_bpr_instance_table`) are not provided and must be created and deployed.
*   **Commented-out Job Management Logic**: The original KornShell script contained commented-out calls to `FOSJobDeaktivate` and `FOSJobErzeugeEintrag`. If this job management functionality is required, the `project.dataset.job_control` table DDL needs to be deployed, and the commented-out `INSERT` logic within `r_ausd_bp_ta_bpr_instance.sql` must be activated and adapted to the specific requirements of the job control system.
*   **Commented-out Post-processing Logic**: The original script also had commented-out blocks for `sed`, `sort`, and `join` operations on various `.dat` and `.csv` files. If these post-processing steps are ever reactivated or become necessary, they will need to be re-implemented using BigQuery SQL (e.g., string manipulation, array functions, window functions, `JOIN`s) or potentially using Cloud Dataflow for complex file-based transformations.
*   **`BERT_DIR_ROOT`, `DW_DIR_UTL` Environment Variables**: These environment variables from the legacy system are replaced by explicit BigQuery project, dataset, and table names. All implicit references to these directory structures must be resolved to their BigQuery equivalents.
*   **`target_bpr_instance_table` and `processing_date_col` Placeholders**: The `r_ausd_bp_ta_bpr_instance` procedure contains placeholders like `project.dataset.target_bpr_instance_table` and `processing_date_col` in the record counting section. These must be replaced with the actual names of the target table and the column used for filtering by date, as defined in the migrated `d_ausd_bp_ta_bpr_instance` procedure.

## 6. Validation

Validation should cover both unit-level testing of the BigQuery procedures and end-to-end integration testing.

### How to run the tests:

1.  **Prerequisites**:
    *   All DDLs (`error_log`, `job_audit`, `job_control` (if used), and the target table for `d_ausd_bp_ta_bpr_instance`) must be deployed.
    *   Both `project.dataset.d_ausd_bp_ta_bpr_instance` (with its migrated SQL logic) and `project.dataset.r_ausd_bp_ta_bpr_instance` must be deployed.
    *   Test data should be loaded into the BigQuery source tables that `d_ausd_bp_ta_bpr_instance` reads from.

2.  **Unit Tests (BigQuery Console / `bq` CLI / API)**:
    *   **Successful Execution**:
        ```sql
        CALL `project.dataset.r_ausd_bp_ta_bpr_instance`('JOB_TEST_01', 'ENTRY_001', '01012023', 0);
        ```
        *   Verify `project.dataset.job_audit` contains a new entry for `JOB_TEST_01` with a non-zero `records` count (assuming data was processed).
        *   Verify the target table (e.g., `project.dataset.target_bpr_instance_table`) contains the expected processed data for `01012023`.
    *   **Invalid `JobKennung`**:
        ```sql
        CALL `project.dataset.r_ausd_bp_ta_bpr_instance`(NULL, 'ENTRY_001', '01012023', 0);
        ```
        *   Expect the procedure to `RAISE` an error.
        *   Verify `project.dataset.error_log` contains an entry with `error_nr = 1001`.
    *   **Invalid `Stichtag` Format**:
        ```sql
        CALL `project.dataset.r_ausd_bp_ta_bpr_instance`('JOB_TEST_02', 'ENTRY_002', '2023-01-01', 0); -- Incorrect format
        ```
        *   Expect the procedure to `RAISE` an error.
        *   Verify `project.dataset.error_log` contains an entry with `error_nr = 1004`.
    *   **Restart Value**:
        ```sql
        CALL `project.dataset.r_ausd_bp_ta_bpr_instance`('JOB_TEST_03', 'ENTRY_003', '02012023', 1);
        ```
        *   Verify `project.dataset.job_audit` (and `job_control` if active) correctly reflects the `p_wiederanlaufWert`.

3.  **Integration Tests (End-to-End)**:
    *   If an external orchestrator (e.g., Airflow) is used, trigger the DAG.
    *   Compare the output data in BigQuery target tables with the output generated by the legacy KornShell script for the same input parameters and source data. This is crucial for data integrity.
    *   Monitor BigQuery job history for successful completion and resource consumption.

### What "passing" means:

*   **Successful Execution**: The `project.dataset.r_ausd_bp_ta_bpr_instance` procedure completes without unhandled errors for valid inputs.
*   **Data Integrity**: The data produced in the BigQuery target tables by `d_ausd_bp_ta_bpr_instance` is accurate and matches the expected output from the legacy system.
*   **Audit Logging**: An entry is successfully created in `project.dataset.job_audit` for each run, containing the correct `job_kennung`, `eintrags_nr`, `stichtag`, and `records` count.
*   **Error Handling**: For invalid input parameters, the procedure correctly raises an error, and a corresponding entry is logged in `project.dataset.error_log` with the appropriate error number and message.
*   **Performance**: The BigQuery job completes within acceptable timeframes and resource limits.
*   **Optional Job Control**: If `job_control` is activated, entries are correctly created/updated in `project.dataset.job_control`.

## 7. Rollback procedure

In case of critical issues or unexpected behavior after go-live, the following rollback procedure can be initiated:

1.  **Deactivate New BigQuery Job**:
    *   If scheduled via Airflow or other orchestrator, pause or disable the DAG/job.
    *   If scheduled via BigQuery Scheduled Queries, disable the scheduled query.
2.  **Re-enable Legacy Job**:
    *   Reactivate the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_instance.ksh` script in the legacy environment. Ensure its scheduling is restored.
3.  **Data Rollback (if necessary)**:
    *   **Identify Impacted Data**: Determine which data was written or modified by the BigQuery job.
    *   **Delete/Restore Data**:
        *   If the BigQuery job created new data in target tables, delete the data inserted during the problematic runs.
        *   If the BigQuery job updated existing data, restore the affected rows from a previous backup or use BigQuery's time travel feature (e.g., `FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL X HOUR)`) to revert tables to a state before the problematic execution.
        *   **Caution**: This step is highly dependent on the specific data manipulation logic within `d_ausd_bp_ta_bpr_instance.sql`. A detailed data rollback plan should be part of the `d_ausd_bp_ta_bpr_instance` migration.
4.  **Revert BigQuery Objects (Optional)**:
    *   If the deployed BigQuery procedures or DDLs are causing issues, they can be reverted:
        *   Drop the `project.dataset.r_ausd_bp_ta_bpr_instance` and `project.dataset.d_ausd_bp_ta_bpr_instance` procedures.
        *   If the `error_log`, `job_audit`, or `job_control` tables were newly created and contain no critical data, they can be dropped. If they contain valuable audit data, they should be retained.
5.  **Post-Rollback Analysis**:
    *   Analyze the root cause of the failure in the BigQuery environment.
    *   Address the identified issues, re-test thoroughly, and plan for a re-deployment.