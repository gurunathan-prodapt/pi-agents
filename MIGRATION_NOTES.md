# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `k_ausd_bp_ta_tarifoption.ksh` from its original environment to Google BigQuery. The original script served as an orchestrator, handling parameter validation, environment setup, and the execution of a core SQL script (`d_ausd_bp_ta_tarifoption.sql`) to process data, likely related to the `PoolBasisprodukt` table.

The migration targets Google BigQuery, leveraging its native SQL stored procedures for both orchestration and core business logic. The shell script's control flow, parameter handling, and logging mechanisms have been translated into BigQuery SQL, while the core data processing logic (originally in `d_ausd_bp_ta_tarifoption.sql`) is intended to be encapsulated within a separate BigQuery stored procedure.

## 2. Generated Artifacts

The migration process has generated the following BigQuery SQL files:

*   **`create_table_poolbasisprodukt.sql`**
    *   **Role:** Defines the Data Definition Language (DDL) for the `PoolBasisprodukt` table in BigQuery. This table is the primary target or source for the core business logic. The schema is a placeholder and must be adjusted to reflect the actual source system's `PoolBasisprodukt` table structure.
*   **`create_table_error_log.sql`**
    *   **Role:** Defines the DDL for a dedicated BigQuery table (`error_log`) used to capture and store detailed error messages and stack traces whenever the migrated job encounters an exception. This replaces the custom shell-based error logging.
*   **`create_table_job_log.sql`**
    *   **Role:** Defines the DDL for a BigQuery table (`job_log`) to record the execution status, start/end times, and processed record counts for each run of the migrated job. This replaces the shell script's job management and logging functionality.
*   **`d_ausd_bp_ta_tarifoption_core.sql`**
    *   **Role:** This file contains a placeholder BigQuery stored procedure (`your_project_id.your_dataset_id.d_ausd_bp_ta_tarifoption_core`) that is intended to encapsulate the core data processing logic. The original content of `d_ausd_bp_ta_tarifoption.sql` must be manually translated into BigQuery SQL and inserted into this procedure. It accepts parameters from the orchestrator and returns the count of processed records.
*   **`r_ausd_bp_ta_tarifoption.sql`**
    *   **Role:** This is the main orchestration BigQuery stored procedure (`your_project_id.your_dataset_id.r_ausd_bp_ta_tarifoption`). It replaces the `k_ausd_bp_ta_tarifoption.ksh` script. Its responsibilities include:
        *   Accepting and validating input parameters (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`).
        *   Deriving necessary dates (e.g., current date, yesterday's date).
        *   Calling the `d_ausd_bp_ta_tarifoption_core` procedure to execute the business logic.
        *   Capturing and logging job status, record counts, and errors to the `job_log` and `error_log` tables.

## 3. Key Design Decisions

*   **Orchestration Shift to BigQuery Stored Procedures:** The primary control flow, parameter validation, and job execution management (originally in `k_ausd_bp_ta_tarifoption.ksh`) have been migrated directly into a BigQuery SQL stored procedure (`r_ausd_bp_ta_tarifoption`). This eliminates the need for an external shell environment and leverages BigQuery's native capabilities for procedural logic.
*   **Core Logic Encapsulation:** The business logic from `d_ausd_bp_ta_tarifoption.sql` is designed to be encapsulated within a separate BigQuery stored procedure (`d_ausd_bp_ta_tarifoption_core`). This promotes modularity and allows for independent development and testing of the core data transformations.
*   **Native BigQuery Features for Utilities:** Shell script utilities for date handling (`h_alis_date.ksh`, `gestern.ksh`) and parameter parsing (`h_alis_parameter.ksh`) are replaced by BigQuery's built-in date functions (e.g., `CURRENT_DATE()`, `DATE_SUB()`, `SAFE.PARSE_DATE()`) and direct stored procedure parameters.
*   **Centralized Logging in BigQuery Tables:** Custom shell-based error reporting (`f_alis_msgerr.ksh`, `DWMSG_MeldeFehler`) and job logging are replaced by dedicated BigQuery tables (`error_log`, `job_log`). This provides a structured, queryable, and scalable logging solution within the data warehouse environment.
*   **Elimination of Temporary Files:** The use of temporary files for capturing record counts (`$tmpFile`) is replaced by BigQuery stored procedure `OUT` parameters and direct `SELECT COUNT(*)` queries or `@@row_count` within the procedural SQL.
*   **Oracle to BigQuery Data Migration (Implied):** The original script's interaction with an Oracle database (via `h_alis_sqlplus.ksh`) implies that all relevant source data will be migrated to BigQuery tables, allowing the core logic to operate natively within BigQuery.

**Notable Trade-offs:**

*   **Manual Translation of Core Logic:** The most significant trade-off is that the content of `d_ausd_bp_ta_tarifoption.sql` requires manual translation and insertion into the `d_ausd_bp_ta_tarifoption_core` BigQuery stored procedure. This is a complex, non-automated step that requires deep understanding of the original SQL and BigQuery SQL syntax.
*   **Loss of Direct Shell Script Flexibility:** While moving to BigQuery stored procedures offers performance and scalability benefits, it removes the flexibility of shell scripting for interacting with external systems or file systems directly. Any such requirements would need to be re-evaluated for BigQuery-native solutions or external orchestration (e.g., Cloud Composer).
*   **Placeholder Schemas:** The DDL for `PoolBasisprodukt` is a placeholder. Its actual schema must be accurately defined based on the source system, which requires manual effort and data analysis.

## 4. Manual Steps Before Go-Live

Before the migrated job can be deployed and run in production, the following manual steps are required:

1.  **BigQuery Dataset Creation:** Ensure the target BigQuery dataset (`your_dataset_id` in `your_project_id.your_dataset_id`) exists. If not, create it.
    ```bash
    bq mk --dataset your_project_id:your_dataset_id
    ```
2.  **Deploy DDL for Logging and Target Tables:**
    *   Execute `create_table_error_log.sql`, `create_table_job_log.sql`, and `create_table_poolbasisprodukt.sql` in your target BigQuery dataset.
    *   **Crucially, update the `create_table_poolbasisprodukt.sql` file** to accurately reflect the schema (column names, data types, nullability) of the `PoolBasisprodukt` table from the source system. The provided DDL is a generic placeholder.
    *   Replace `your_project_id.your_dataset_id` placeholders with your actual project and dataset IDs.
3.  **Translate and Deploy Core Business Logic:**
    *   **Manually translate the entire content of the original `d_ausd_bp_ta_tarifoption.sql` file into BigQuery SQL.** This is the most critical and complex manual step.
    *   Insert the translated BigQuery SQL into the `d_ausd_bp_ta_tarifoption_core.sql` file, replacing the placeholder comments. Ensure the `OUT record_count INT64` parameter correctly captures the total number of records processed by the core logic.
    *   Deploy the updated `d_ausd_bp_ta_tarifoption_core.sql` as a BigQuery stored procedure.
    *   Replace `your_project_id.your_dataset_id` placeholders with your actual project and dataset IDs.
4.  **Deploy Orchestration Stored Procedure:**
    *   Deploy the `r_ausd_bp_ta_tarifoption.sql` file as a BigQuery stored procedure.
    *   Replace `your_project_id.your_dataset_id` placeholders with your actual project and dataset IDs.
5.  **IAM Permissions:**
    *   Ensure the service account or user identity that will execute the `r_ausd_bp_ta_tarifoption` stored procedure has the necessary BigQuery IAM roles:
        *   `BigQuery Data Editor` (or equivalent) on `your_project_id.your_dataset_id` to write to `job_log`, `error_log`, and `PoolBasisprodukt` (and any other tables modified by `d_ausd_bp_ta_tarifoption_core`).
        *   `BigQuery Job User` to run BigQuery jobs.
        *   `BigQuery Data Viewer` (or equivalent) on any source tables accessed by `d_ausd_bp_ta_tarifoption_core`.
6.  **Data Migration:** Ensure all source data required by `d_ausd_bp_ta_tarifoption_core` (e.g., the original `PoolBasisprodukt` data) has been successfully migrated and loaded into the corresponding BigQuery tables.
7.  **Scheduling:**
    *   **If using Cloud Composer/Airflow:** Create and deploy a new DAG (`k_ausd_bp_ta_tarifoption_dag.py` if generated) that calls the `your_project_id.your_dataset_id.r_ausd_bp_ta_tarifoption` stored procedure with the required parameters.
    *   **If direct execution:** Configure a scheduled query in BigQuery, a Cloud Function, or a script (e.g., using `bq query --run_as_user=... --project_id=... --dataset_id=... 'CALL your_dataset_id.r_ausd_bp_ta_tarifoption(...)'`) to execute the procedure at the desired frequency.

## 5. Known Gaps & Unresolved References

The following items were identified as gaps or risks during the migration design and require further attention:

*   **`d_ausd_bp_ta_tarifoption.sql` Content:** The most critical unresolved item is the actual content of `d_ausd_bp_ta_tarifoption.sql`. This file contains the core business logic, including specific DML/DDL statements, data sources, and targets. A separate, detailed analysis and manual translation of this SQL script into BigQuery SQL is absolutely essential to complete the overall job migration. The `d_ausd_bp_ta_tarifoption_core.sql` file is currently a placeholder.
*   **Orchestration Details of `d_ausd_bp_ta_tarifoption.sql`:** The `starteSQLSkript` function in the original KornShell script is a wrapper. The exact parameters, error handling, and transactional behavior of the original SQL script's execution (as defined in `h_alis_sqlplus.ksh` and `d_ausd_bp_ta_tarifoption.sql`) need to be fully understood to ensure a faithful and robust BigQuery migration, especially regarding transaction management and commit/rollback logic.
*   **Commented-out Logic:** The original script contains commented-out `sed`, `sort`, and `join` operations. While currently inactive, there is a risk that this logic might be unexpectedly required later. Clarification on its necessity and potential re-activation is needed. If required, this logic would need to be translated into BigQuery SQL transformations.
*   **Job Management Functions:** `FOSJobDeaktivate` and `FOSJobErzeugeEintrag` were commented out in the original script. If these job management functions are active in other parts of the source system and are critical for this job's lifecycle (e.g., for external monitoring or dependency management), their functionality will need to be implemented either as BigQuery logging, part of a Cloud Composer/Dataform orchestration, or within a separate application.
*   **`p_wiederanlaufWert` Usage:** The `p_wiederanlaufWert` parameter is initialized but not explicitly used in the provided KornShell script content. Its intended use within the original `d_ausd_bp_ta_tarifoption.sql` needs to be understood and incorporated into the `d_ausd_bp_ta_tarifoption_core` BigQuery stored procedure if it influences the core logic.

## 6. Validation

To validate the successful migration and functionality of the BigQuery job, follow these steps:

1.  **Prerequisites:** Ensure all manual steps (Section 4) have been completed, especially the translation and deployment of `d_ausd_bp_ta_tarifoption_core.sql`.
2.  **Execute the Orchestration Procedure:**
    *   Call the main orchestration stored procedure `your_project_id.your_dataset_id.r_ausd_bp_ta_tarifoption` with representative test parameters.
    *   Example using `bq query`:
        ```bash
        bq query --project_id=your_project_id --dataset_id=your_dataset_id \
        'CALL your_dataset_id.r_ausd_bp_ta_tarifoption("JOB123", "ENTRY001", "01012023", "RESTART_VAL");'
        ```
    *   If using Cloud Composer, trigger the corresponding DAG.
3.  **Monitor Execution:** Observe the BigQuery job execution in the GCP Console.
4.  **Check `job_log` Table:**
    *   Query `your_project_id.your_dataset_id.job_log` for the `run_id` corresponding to your test execution.
    *   **Passing Criteria:** The `status` column for the latest entry of your `job_id` and `run_id` should be `SUCCEEDED`. The `record_count` should reflect the expected number of records processed by the `d_ausd_bp_ta_tarifoption_core` procedure.
5.  **Check `error_log` Table:**
    *   Query `your_project_id.your_dataset_id.error_log` for the `run_id` corresponding to your test execution.
    *   **Passing Criteria:** There should be *no* entries in the `error_log` table for the successful test run.
6.  **Verify Target Data:**
    *   Query the target tables, especially `your_project_id.your_dataset_id.PoolBasisprodukt`, to confirm that data has been inserted, updated, or merged correctly as per the business logic defined in `d_ausd_bp_ta_tarifoption_core`.
    *   Compare the state of the target tables before and after the run, and against expected outcomes based on the source data.
    *   Perform data quality checks to ensure data integrity and accuracy.

## 7. Rollback Procedure

In the event of critical issues or unexpected behavior after deploying the migrated BigQuery job, the following rollback procedure should be followed:

1.  **Stop New BigQuery Executions:**
    *   Immediately disable or pause any scheduled executions (e.g., Cloud Composer DAGs, BigQuery Scheduled Queries) of the `your_project_id.your_dataset_id.r_ausd_bp_ta_tarifoption` stored procedure.
    *   Inform any manual users to cease direct calls to the procedure.
2.  **Revert to Original System:**
    *   Re-enable or resume the execution of the original `k_ausd_bp_ta_tarifoption.ksh` script in its source environment. Ensure the original scheduling and dependencies are restored.
3.  **Data Reversion (Conditional):**
    *   **Assess Impact:** Determine if the migrated BigQuery job made any irreversible data modifications to production BigQuery tables (e.g., `PoolBasisprodukt`).
    *   **If data was modified:** Depending on the nature of the modifications and the business requirements, a data rollback strategy may be necessary. This could involve:
        *   Restoring tables from a previous snapshot or backup (if available).
        *   Executing compensating transactions to revert specific changes.
        *   Using BigQuery's time travel feature to query data as it was before the erroneous run and then restoring it.
    *   **If data was not modified (e.g., only inserted into new tables or temporary tables):** No data reversion might be necessary, but verify the state of all affected tables.
4.  **Remove BigQuery Artifacts (Optional, for clean slate):**
    *   If a complete rollback and re-migration is planned, consider dropping the deployed BigQuery stored procedures (`r_ausd_bp_ta_tarifoption`, `d_ausd_bp_ta_tarifoption_core`) and potentially the logging tables (`error_log`, `job_log`) and the `PoolBasisprodukt` table (if it was created solely for this migration).
    ```bash
    bq rm -f --routine your_project_id:your_dataset_id.r_ausd_bp_ta_tarifoption
    bq rm -f --routine your_project_id:your_dataset_id.d_ausd_bp_ta_tarifoption_core
    bq rm -f your_project_id:your_dataset_id.error_log
    bq rm -f your_project_id:your_dataset_id.job_log
    bq rm -f your_project_id:your_dataset_id.PoolBasisprodukt
    ```
5.  **Root Cause Analysis:** Investigate the reason for the rollback, address the identified issues (e.g., bugs in translated SQL, incorrect configuration, missing data), and re-plan the migration.