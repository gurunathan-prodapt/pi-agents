# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the `k_ausd_v_ta_vvl_upgrade.ksh` KornShell script and its associated SQL logic. The original script orchestrated the execution of a SQL job (`d_ausd_v_ta_vvl_upgrade.sql`) to update the `ta_vvl_upgrade` table, handling environment setup, parameter validation, error handling, and job status management.

The job has been migrated from a legacy on-premise environment (KornShell, Oracle SQL) to **Google Cloud's BigQuery platform**. The orchestration logic is re-implemented as BigQuery Stored Procedures, and the data transformation logic is expected to be converted into BigQuery-compliant SQL within a dedicated stored procedure.

## 2. Generated Artifacts

The migration process generated the following BigQuery SQL artifacts:

*   **`project/dataset/ddl/job_error_log.sql`**
    *   **Role:** Data Definition Language (DDL) for creating the `job_error_log` table. This table serves as a centralized repository for logging errors encountered during the execution of the migrated BigQuery stored procedures, replacing the error reporting functionality previously handled by `f_alis_msgerr.ksh`.
*   **`project/dataset/ddl/job_run_log.sql`**
    *   **Role:** DDL for creating the `job_run_log` table. This table records the start, end, status, and processed record counts for each execution of the BigQuery job, providing an audit trail and operational visibility.
*   **`project/dataset/ddl/job_table.sql`**
    *   **Role:** DDL for creating the `job_table`. This table manages the active status of jobs, replicating the logic from the original `starteSQLSkript` function (within `h_alis_sqlplus.ksh`) that prevented concurrent runs or managed job states.
*   **`project/dataset/procedures/d_ausd_v_ta_vvl_upgrade.sql`**
    *   **Role:** BigQuery Stored Procedure. This procedure is a placeholder for the core data transformation logic originally found in `d_ausd_v_ta_vvl_upgrade.sql`. It is designed to perform the actual data updates/merges on the `ta_vvl_upgrade` table and return the count of affected records. **Note: The current version is a placeholder and needs to be populated with the actual BigQuery-compliant SQL logic.**
*   **`project/dataset/procedures/r_ausd_vertrag_control.sql`**
    *   **Role:** BigQuery Stored Procedure. This is the main orchestration procedure, replacing the `k_ausd_v_ta_vvl_upgrade.ksh` script. It handles parameter validation, job status management (checking for active jobs, deactivating old ones), logging job runs, calling the `d_ausd_v_ta_vvl_upgrade` procedure, and comprehensive error handling.

## 3. Key Design Decisions

*   **Orchestration Re-platforming:** The KornShell script's orchestration logic (parameter parsing, validation, job status, error handling) was migrated directly into a BigQuery Stored Procedure (`r_ausd_vertrag_control`).
    *   **Why:** This approach leverages BigQuery's native scripting capabilities, reducing external dependencies and enabling a fully managed, serverless solution within BigQuery. It simplifies deployment and maintenance compared to external orchestrators for simple control flows.
    *   **Trade-offs:** While BigQuery scripting is powerful, it might be less flexible for complex external interactions (e.g., calling external APIs, file system operations) compared to a full-fledged orchestrator like Cloud Composer. For this specific job, the internal nature of the orchestration made BigQuery scripting a suitable choice.
*   **Data Transformation Encapsulation:** The core SQL logic from `d_ausd_v_ta_vvl_upgrade.sql` is intended to be encapsulated within a separate BigQuery Stored Procedure (`d_ausd_v_ta_vvl_upgrade`).
    *   **Why:** This promotes modularity, reusability, and easier testing of the data transformation logic independently from the orchestration. It also allows for clear separation of concerns.
    *   **Trade-offs:** The content of the original `d_ausd_v_ta_vvl_upgrade.sql` was not provided, leading to a placeholder procedure. The actual migration effort for this part could be significant depending on Oracle-specific syntax and features used.
*   **Centralized Logging and Job Status Management:** Dedicated BigQuery tables (`job_error_log`, `job_run_log`, `job_table`) were introduced to manage job status, logging, and error reporting.
    *   **Why:** This replaces disparate shell-based logging, temporary files, and implicit job status mechanisms with structured, queryable, and persistent metadata within BigQuery. This improves observability, auditing, and debugging.
    *   **Trade-offs:** Introduces new BigQuery table dependencies. The `job_table` uses a `PRIMARY KEY (...) NOT ENFORCED` constraint, which is BigQuery's way of defining primary keys for metadata purposes, but it doesn't guarantee uniqueness at the database level like in traditional RDBMS. The application logic must ensure data integrity for job status.
*   **Parameter Handling:** Command-line arguments (`p_JobKennung`, `p_EintragsNr`) are directly mapped to `IN` parameters of the BigQuery Stored Procedure.
    *   **Why:** This is the natural way to pass parameters to BigQuery procedures, maintaining the original job's configurability.
    *   **Trade-offs:** Requires external callers (e.g., `bq query` command, Cloud Composer) to explicitly pass these parameters.
*   **Error Handling:** BigQuery's `RAISE USING MESSAGE` and `EXCEPTION WHEN ERROR` blocks are used for robust error handling, with details logged to `job_error_log`.
    *   **Why:** Provides structured error reporting and allows for graceful failure and logging, improving reliability.

## 4. Manual Steps Before Go-Live

Before the migrated job can be run in production, the following manual steps must be completed:

1.  **BigQuery Project and Dataset Setup:**
    *   Ensure the target Google Cloud Project (`project`) and BigQuery Dataset (`dataset`) exist. If not, create them.
2.  **IAM Permissions:**
    *   Grant appropriate IAM roles to the service account or user that will execute the BigQuery stored procedures. This typically includes:
        *   `BigQuery Data Editor` (for `project.dataset`) to write to log tables and `ta_vvl_upgrade`.
        *   `BigQuery Job User` to run queries and procedures.
        *   `BigQuery Data Viewer` (for source datasets) to read from source tables.
3.  **Deploy DDLs:**
    *   Execute the DDL scripts to create the necessary metadata tables:
        *   `project/dataset/ddl/job_error_log.sql`
        *   `project/dataset/ddl/job_run_log.sql`
        *   `project/dataset/ddl/job_table.sql`
    *   Example command: `bq query --use_legacy_sql=false < project/dataset/ddl/job_error_log.sql`
4.  **Migrate Source and Target Data:**
    *   Ensure all source tables referenced by the original `d_ausd_v_ta_vvl_upgrade.sql` (which are currently unknown) are migrated to BigQuery.
    *   Migrate the target table `ta_vvl_upgrade` to BigQuery, including its schema and historical data if required.
5.  **Populate `d_ausd_v_ta_vvl_upgrade.sql`:**
    *   **Crucially, replace the placeholder logic within `project/dataset/procedures/d_ausd_v_ta_vvl_upgrade.sql` with the actual BigQuery-compliant SQL transformation logic from the original `d_ausd_v_ta_vvl_upgrade.sql` file.** This involves converting any Oracle-specific syntax, functions, or PL/SQL constructs to their BigQuery equivalents.
6.  **Deploy Stored Procedures:**
    *   Execute the stored procedure creation scripts:
        *   `project/dataset/procedures/d_ausd_v_ta_vvl_upgrade.sql` (after populating its logic)
        *   `project/dataset/procedures/r_ausd_vertrag_control.sql`
    *   Example command: `bq query --use_legacy_sql=false < project/dataset/procedures/r_ausd_vertrag_control.sql`
7.  **Scheduling:**
    *   Integrate the execution of `project.dataset.r_ausd_vertrag_control` into your chosen BigQuery scheduler. This could be:
        *   **Cloud Composer (Apache Airflow):** Create a DAG that calls the BigQuery stored procedure using the `BigQueryExecuteStoredProcedureOperator`.
        *   **Google Cloud Workflows:** Define a workflow that invokes the BigQuery procedure.
        *   **Dataform:** If this is part of a larger data pipeline managed by Dataform, define a Dataform `operation` to call the procedure.
        *   **Manual `bq query` command:** For ad-hoc or simple cron-based scheduling.
    *   Ensure the scheduler passes the required `p_job_kennung` and `p_eintrags_nr` parameters.

## 5. Known Gaps & Unresolved References

*   **Core SQL Logic (`d_ausd_v_ta_vvl_upgrade.sql`):** The most significant gap is the actual content of the `d_ausd_v_ta_vvl_upgrade.sql` file. The generated `project/dataset/procedures/d_ausd_v_ta_vvl_upgrade.sql` is a placeholder. This procedure **must be manually populated** with the BigQuery-compliant transformation logic. This may involve:
    *   Converting Oracle-specific SQL functions (e.g., `NVL`, `DECODE`, `TO_DATE` formats) to BigQuery equivalents (`IFNULL`, `CASE`, `PARSE_DATE`).
    *   Refactoring PL/SQL blocks into BigQuery scripting or separate procedures/UDFs.
    *   Ensuring data types are compatible and handled correctly.
*   **Utility Script Functionality:** While the core logic of `h_alis_parameter.ksh` (validation) and `f_alis_msgerr.ksh` (error logging) has been replicated, the specific functionalities of `h_alis_date.ksh` (date utilities) and any other sourced scripts were not fully detailed. If these scripts contained complex, custom date manipulations or other business logic, those specific parts might need further analysis and BigQuery implementation.
*   **External Orchestration Details:** The migration design assumes an external orchestrator (e.g., Cloud Composer, Workflows, Dataform) will call the main BigQuery stored procedure. The specific implementation of this external orchestration (e.g., DAGs, workflow definitions) is outside the scope of the generated artifacts and needs to be developed separately.
*   **`job_table` Primary Key Enforcement:** BigQuery's `PRIMARY KEY NOT ENFORCED` means that while the key is defined for metadata, the database does not prevent duplicate entries. The application logic within `r_ausd_vertrag_control` handles this by `MERGE` operations, but it's a conceptual difference from traditional RDBMS.

## 6. Validation

Validation ensures the migrated job functions correctly and produces accurate results.

### How to Run Tests

1.  **Prerequisites:** Ensure all manual steps (Section 4) are completed, especially the population of `d_ausd_v_ta_vvl_upgrade.sql` with actual logic.
2.  **Execute the Main Procedure:** Call the `project.dataset.r_ausd_vertrag_control` stored procedure using the `bq query` command or your chosen orchestration tool (e.g., Cloud Composer).

    ```bash
    # Example using bq command-line tool
    bq query --project_id=<your-gcp-project> --location=<your-bq-location> --use_legacy_sql=false \
    'CALL project.dataset.r_ausd_vertrag_control("JOB_AUSD_VVL", "ENTRY_001");'
    ```
3.  **Test Cases:**
    *   **Successful Run:** Execute with valid `p_job_kennung` and `p_eintrags_nr`.
    *   **Parameter Validation Failure:** Execute with `NULL` or empty `p_job_kennung` or `p_eintrags_nr`.
    *   **Job Already Active:** Execute the same job twice in quick succession. The second run should be skipped.
    *   **Core Logic Failure:** (If possible) Introduce a deliberate error in `d_ausd_v_ta_vvl_upgrade` to test error handling.
    *   **Data Validation:** Run the migrated job against a representative dataset and compare the output in `ta_vvl_upgrade` with the expected results from the legacy system.

### What "Passing" Means

A successful validation indicates the migration is complete and functional:

*   **Procedure Completion:** The `CALL` statement for `r_ausd_vertrag_control` completes without raising an unhandled error.
*   **`job_run_log` Status:**
    *   For successful runs: An entry exists in `project.dataset.job_run_log` with `status = 'COMPLETED'`, `end_timestamp` populated, and `processed_records` reflecting the actual number of rows affected by `d_ausd_v_ta_vvl_upgrade`.
    *   For skipped runs (due to active job): An entry exists with `status = 'SKIPPED'`.
    *   For failed runs: An entry exists with `status = 'FAILED'`.
*   **`job_error_log` Content:**
    *   For successful runs: No new entries related to this job in `project.dataset.job_error_log`.
    *   For runs with invalid parameters or core logic failures: An entry exists in `project.dataset.job_error_log` with relevant `error_code` and `error_message`.
*   **`job_table` Status:**
    *   After a successful run, the corresponding entry in `project.dataset.job_table` should have `job_status = 'INACTIVE'`.
    *   During an active run, `job_status` should be `ACTIVE`.
*   **Data Accuracy:** The data in the target `project.dataset.ta_vvl_upgrade` table, after the job execution, must match the expected output based on the original system's logic and source data. This is the most critical validation step.
*   **Performance:** The execution time of the BigQuery stored procedure should be within acceptable performance thresholds, ideally outperforming the legacy system.

## 7. Rollback Procedure

In case of critical issues or unexpected behavior after go-live, the following rollback procedure can be initiated:

1.  **Stop New Executions:** Immediately halt any scheduled or manual executions of the `project.dataset.r_ausd_vertrag_control` BigQuery stored procedure. This might involve pausing Cloud Composer DAGs, Dataform jobs, or removing cron entries.
2.  **Revert to Legacy System:** Re-enable the original `k_ausd_v_ta_vvl_upgrade.ksh` script and its associated dependencies in the legacy environment. Ensure the legacy scheduler is re-activated.
3.  **Data Reversion (if necessary):**
    *   If the migrated BigQuery job made irreversible data changes to `project.dataset.ta_vvl_upgrade` that are not compatible with the legacy system or caused data corruption, consider using BigQuery's [Time Travel](https://cloud.google.com/bigquery/docs/data-manipulation-language#time_travel) feature to restore `ta_vvl_upgrade` to a state before the problematic BigQuery job run.
    *   Alternatively, restore `ta_vvl_upgrade` from a backup taken before the BigQuery job's first production run.
    *   **Note:** This step is highly dependent on the nature of the data changes and the criticality of data consistency between systems during rollback.
4.  **Isolate BigQuery Objects:**
    *   (Optional, for clean rollback) Drop the BigQuery stored procedures and DDL tables created during the migration:
        *   `DROP PROCEDURE IF EXISTS project.dataset.r_ausd_vertrag_control;`
        *   `DROP PROCEDURE IF EXISTS project.dataset.d_ausd_v_ta_vvl_upgrade;`
        *   `DROP TABLE IF EXISTS project.dataset.job_error_log;`
        *   `DROP TABLE IF EXISTS project.dataset.job_run_log;`
        *   `DROP TABLE IF EXISTS project.dataset.job_table;`
    *   This ensures a clean slate if a re-migration attempt is planned.
5.  **Root Cause Analysis:** Investigate the reason for the rollback, address the identified issues (e.g., bugs in BigQuery SQL, performance bottlenecks, incorrect logic), and plan for a revised migration.