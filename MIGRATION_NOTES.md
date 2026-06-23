# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell job `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_apn_ve.ksh`. The original script orchestrated the execution of an external SQL file (`d_ausd_v_ta_apn_ve.sql`), handled job control (activation/deactivation), parameter validation, and basic logging.

The job has been migrated to Google Cloud Platform, primarily utilizing **BigQuery Stored Procedures** for logic encapsulation and **BigQuery Tables** for persistent logging and job state management. Orchestration of the migrated BigQuery components is handled by an **Apache Airflow DAG** deployed on Cloud Composer.

## 2. Generated artifacts

The migration process generated the following files, each serving a specific role in the new BigQuery-centric architecture:

*   **`project/dataset/create_error_log_table.sql`**
    *   **Role:** SQL script to create the `error_log` table in BigQuery. This table centralizes error messages, replacing the shell script's ad-hoc error reporting and `f_alis_msgerr.ksh` utility.
*   **`project/dataset/create_job_table.sql`**
    *   **Role:** SQL script to create the `job_table` in BigQuery. This table manages the state (e.g., ACTIVE, RUNNING, COMPLETED) of jobs, replacing the shell script's internal job control mechanisms.
*   **`project/dataset/create_job_run_audit_table.sql`**
    *   **Role:** SQL script to create the `job_run_audit` table in BigQuery. This table logs execution details, record counts, and status for each job run, replacing temporary files and implicit job tracking.
*   **`project/dataset/create_target_table_for_ta_apn_ve.sql`**
    *   **Role:** SQL script to create a placeholder `target_table_for_ta_apn_ve` in BigQuery. This table represents the ultimate destination for the data processed by the migrated SQL logic. Its schema needs to be finalized based on the actual content of the original `d_ausd_v_ta_apn_ve.sql`.
*   **`project/dataset/d_ausd_v_ta_apn_ve.sql`**
    *   **Role:** BigQuery Stored Procedure. This procedure is a placeholder for the core data processing logic originally found in `d_ausd_v_ta_apn_ve.sql`. Its content must be translated from the original SQL dialect (likely Oracle) into BigQuery SQL. It is responsible for performing the actual data transformations and returning the count of processed records.
*   **`project/dataset/starte_sql_skript.sql`**
    *   **Role:** BigQuery Stored Procedure. This procedure encapsulates the job control logic, including deactivating older jobs, checking for active jobs, and invoking the `d_ausd_v_ta_apn_ve` procedure. It replaces the `starteSQLSkript` function and related job control within the original KornShell script.
*   **`project/dataset/r_ausd_vertrag_control.sql`**
    *   **Role:** Main BigQuery Stored Procedure. This procedure serves as the primary entry point for the migrated job. It handles parameter validation, orchestrates calls to `starte_sql_skript`, and manages audit logging. It replaces the main control flow of the original `k_ausd_v_ta_apn_ve.ksh` script.
*   **`airflow/dags/r_ausd_vertrag_control_dag.py`**
    *   **Role:** Apache Airflow DAG. This Python script defines the workflow for scheduling and executing the `r_ausd_vertrag_control` BigQuery Stored Procedure. It replaces the original shell script's role as the scheduled entry point.

## 3. Key design decisions

*   **Migration to BigQuery Stored Procedures for Logic Encapsulation:**
    *   **Why:** BigQuery Stored Procedures provide a robust, scalable, and maintainable way to encapsulate the procedural logic (parameter parsing, conditional execution, job control) previously handled by the KornShell script. This leverages BigQuery's native capabilities and eliminates the need for external compute for orchestration.
    *   **Trade-offs:** Requires translation of shell scripting constructs (e.g., `if`, `case`, `getopts`, environment variables) into BQSQL. Direct interaction with the file system or external utilities is no longer possible, necessitating BigQuery table-based replacements for logging and job state.
*   **Dedicated BigQuery Tables for State Management and Logging:**
    *   **Why:** Replacing temporary files, shell variables, and custom logging utilities with structured BigQuery tables (`error_log`, `job_table`, `job_run_audit`) provides persistent, queryable, and scalable storage for operational data. This improves observability, auditing, and debugging capabilities.
    *   **Trade-offs:** Requires upfront schema definition and DML operations within stored procedures to manage these tables, adding complexity compared to simple shell file operations.
*   **Modular Stored Procedure Design:**
    *   **Why:** The logic is split into `r_ausd_vertrag_control` (main entry, validation, audit), `starte_sql_skript` (job control, core SQL invocation), and `d_ausd_v_ta_apn_ve` (core data processing). This promotes modularity, reusability, and easier maintenance by separating concerns.
    *   **Trade-offs:** Increased number of BigQuery objects to manage and deploy.
*   **Apache Airflow for Orchestration:**
    *   **Why:** Airflow (via Cloud Composer) is chosen for scheduling, monitoring, and parameterizing the BigQuery Stored Procedure execution. It provides a cloud-native, robust, and feature-rich platform for workflow management, replacing cron-based scheduling and manual execution.
    *   **Trade-offs:** Introduces a new technology stack (Python, Airflow concepts) and requires managing an Airflow environment.
*   **Parameter Handling:**
    *   **Why:** Original command-line arguments (`p_JobKennung`, `p_EintragsNr`) are directly mapped to BigQuery Stored Procedure parameters and subsequently to Airflow DAG parameters. This maintains clarity and explicit input handling.
    *   **Trade-offs:** Requires careful management of parameter values in the Airflow DAG, potentially using Airflow Variables or dynamic context.

## 4. Manual steps before go-live

Before the migrated job can be run in production, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:**
    *   Ensure the target BigQuery dataset (`project.dataset`) exists. If not, create it:
        ```bash
        bq mk --project_id=your-gcp-project-id your_dataset_id
        ```
2.  **IAM Permissions:**
    *   The service account used by Cloud Composer (or any other orchestrator) must have the following BigQuery roles:
        *   `BigQuery Data Editor` (or `BigQuery Data Owner`) on `project.dataset` to create/update tables and execute stored procedures.
        *   `BigQuery Job User` on `your-gcp-project-id` to run BigQuery jobs.
    *   Ensure the user deploying the BigQuery objects has sufficient permissions (e.g., `BigQuery Admin`).
3.  **Deploy BigQuery Tables:**
    *   Execute the `CREATE TABLE` statements for the audit and logging tables:
        ```bash
        bq query --use_legacy_sql=false < project/dataset/create_error_log_table.sql
        bq query --use_legacy_sql=false < project/dataset/create_job_table.sql
        bq query --use_legacy_sql=false < project/dataset/create_job_run_audit_table.sql
        ```
    *   **Crucially, define and create the actual schema for `target_table_for_ta_apn_ve`** based on the original `d_ausd_v_ta_apn_ve.sql`'s output. The provided `create_target_table_for_ta_apn_ve.sql` is a placeholder.
        ```bash
        bq query --use_legacy_sql=false < project/dataset/create_target_table_for_ta_apn_ve.sql
        ```
4.  **Migrate and Deploy Core SQL Logic:**
    *   **Analyze `d_ausd_v_ta_apn_ve.sql`:** Thoroughly review the original SQL file. Translate any Oracle-specific syntax, functions, or PL/SQL blocks into BigQuery SQL. Identify source tables and their BigQuery equivalents.
    *   **Populate `project/dataset/d_ausd_v_ta_apn_ve.sql`:** Replace the `TODO` section in the generated `d_ausd_v_ta_apn_ve.sql` with the translated BigQuery SQL logic. Ensure it correctly reads from source tables and writes to `target_table_for_ta_apn_ve`, and accurately sets the `records_processed` OUT parameter.
    *   **Deploy the Stored Procedure:**
        ```bash
        bq query --use_legacy_sql=false < project/dataset/d_ausd_v_ta_apn_ve.sql
        ```
5.  **Deploy BigQuery Control Stored Procedures:**
    *   Deploy the `starte_sql_skript` and `r_ausd_vertrag_control` procedures:
        ```bash
        bq query --use_legacy_sql=false < project/dataset/starte_sql_skript.sql
        bq query --use_legacy_sql=false < project/dataset/r_ausd_vertrag_control.sql
        ```
6.  **Deploy Airflow DAG:**
    *   Upload `airflow/dags/r_ausd_vertrag_control_dag.py` to your Cloud Composer environment's DAGs folder.
    *   **Configure DAG Parameters:** Edit the `r_ausd_vertrag_control_dag.py` file to replace placeholder values:
        *   `project_id="project"` with your actual GCP project ID.
        *   `dataset_id="dataset"` with your actual BigQuery dataset ID.
        *   `p_JobKennung` and `p_EintragsNr` with appropriate dynamic values (e.g., from Airflow Variables, XComs, or a fixed value if applicable).
    *   **Set Schedule:** Define the `schedule` parameter in the DAG to match the original job's execution frequency.

## 5. Known gaps & unresolved references

*   **Core SQL Logic in `d_ausd_v_ta_apn_ve.sql` (B4 Item):** The most significant gap is the actual content of the `project.dataset.d_ausd_v_ta_apn_ve` stored procedure. The provided code is a placeholder. The original `d_ausd_v_ta_apn_ve.sql` file needs to be thoroughly analyzed, and its logic translated from its original SQL dialect (likely Oracle) into BigQuery SQL. This includes:
    *   Identifying all source tables and their BigQuery equivalents.
    *   Translating specific functions, data types, and procedural constructs.
    *   Ensuring the `records_processed` OUT parameter correctly reflects the number of affected rows.
*   **`target_table_for_ta_apn_ve` Schema Definition (B4 Item):** The schema for `project.dataset.target_table_for_ta_apn_ve` is currently a placeholder. Its definitive structure must be derived from the output of the original `d_ausd_v_ta_apn_ve.sql` script.
*   **Job Management Semantics Validation:** The exact business rules for "ignoring active jobs" and "deactivating older active jobs" (especially the criteria for "older") need to be thoroughly reviewed and validated against the original system's behavior to ensure the `starte_sql_skript` procedure accurately replicates the logic.
*   **Dynamic Airflow Parameters:** The `p_JobKennung` and `p_EintragsNr` parameters in the Airflow DAG are currently hardcoded placeholders. A strategy for dynamically sourcing these values (e.g., from Airflow Variables, a configuration table, or upstream tasks) needs to be implemented.
*   **Original `file_complexity` Data:** The absence of `file_complexity` data for the original `k_ausd_v_ta_apn_ve.ksh` means that potential hidden complexities or specific migration challenges might not have been fully identified during the initial analysis.
*   **Error Handling Granularity:** While basic error logging is in place, the original `f_alis_msgerr.ksh` might have provided more specific error codes or contextual information. Further refinement of error messages and codes in the BigQuery procedures might be necessary.

## 6. Validation

Validation should cover both unit-level testing of individual components and end-to-end integration testing.

### How to run the tests:

1.  **Unit Test BigQuery Stored Procedures:**
    *   **`d_ausd_v_ta_apn_ve`:** Once the core SQL logic is implemented, create mock source tables with representative data. Call the procedure with various `p_EintragsNr` and `p_JobKennung` values and verify the data in `target_table_for_ta_apn_ve` and the `records_processed` output.
    *   **`starte_sql_skript`:** Test different scenarios by pre-populating `project.dataset.job_table`:
        *   Call when no active job exists: Verify `job_table` status transitions (RUNNING -> COMPLETED) and `job_run_audit` entry.
        *   Call when an active job with the *same* `JobKennung` and `EintragsNr` exists: Verify `job_status` is 'IGNORED' and `job_run_audit` reflects this.
        *   Call when an active job with the *same* `JobKennung` but *different* `EintragsNr` exists: Verify the older job is deactivated and the new one runs.
        *   Test error propagation by forcing an error in `d_ausd_v_ta_apn_ve` (e.g., by trying to insert invalid data) and verifying `job_table` and `job_run_audit` reflect 'FAILED' status, and `error_log` contains the error.
    *   **`r_ausd_vertrag_control`:** Call with valid and invalid parameters (missing `p_JobKennung` or `p_EintragsNr`). Verify `error_log` entries for invalid parameters and `job_run_audit` entries for successful/failed runs.
    *   **Example BigQuery CLI call for `r_ausd_vertrag_control`:**
        ```bash
        bq query --use_legacy_sql=false \
        "CALL project.dataset.r_ausd_vertrag_control('TEST_JOB', '12345');"
        ```
2.  **End-to-End Integration Test (via Airflow):**
    *   Trigger the `r_ausd_vertrag_control_dag` in your Cloud Composer environment.
    *   Monitor the Airflow UI for task success/failure.
    *   Verify the BigQuery tables:
        *   Check `project.dataset.job_run_audit` for a successful entry with correct `records_processed`.
        *   Check `project.dataset.job_table` for the final status of the job.
        *   Check `project.dataset.target_table_for_ta_apn_ve` to ensure data was processed correctly and matches expected output.
        *   Check `project.dataset.error_log` if any errors are expected or occur.

### What "passing" means:

*   **Successful Execution:** The Airflow DAG completes successfully, and all BigQuery Stored Procedure calls return without unhandled errors.
*   **Data Integrity:** The `project.dataset.target_table_for_ta_apn_ve` contains the expected data, matching the output of the original `d_ausd_v_ta_apn_ve.sql` script when run with the same inputs.
*   **Accurate Record Counts:** The `records_processed` value in `project.dataset.job_run_audit` accurately reflects the number of records processed or affected by the `d_ausd_v_ta_apn_ve` procedure.
*   **Correct Job State Management:**
    *   `project.dataset.job_table` correctly reflects the job's lifecycle (e.g., 'RUNNING' during execution, 'COMPLETED' upon success, 'FAILED' upon error).
    *   Jobs with the same `JobKennung` and `EintragsNr` are correctly 'IGNORED' if already active.
    *   Older active jobs with the same `JobKennung` but different `EintragsNr` are correctly 'INACTIVE'd.
*   **Comprehensive Logging:**
    *   `project.dataset.job_run_audit` contains a complete and accurate history of all job runs, including start/end times, status, and record counts.
    *   `project.dataset.error_log` captures all expected error conditions with relevant details.
*   **Parameter Validation:** Invalid input parameters result in an error logged to `error_log` and the procedure terminating gracefully.

## 7. Rollback procedure

In case of critical issues or unexpected behavior after deployment, the following rollback procedure can be followed to revert to the original system:

1.  **Disable/Delete Airflow DAG:**
    *   In the Cloud Composer UI, disable or delete the `r_ausd_vertrag_control_dag`. This immediately stops any further scheduled executions of the migrated job.
2.  **Revert BigQuery Stored Procedures:**
    *   If previous versions of the stored procedures (`r_ausd_vertrag_control`, `starte_sql_skript`, `d_ausd_v_ta_apn_ve`) exist, revert to them. Otherwise, delete the newly deployed procedures.
    *   **Note:** Deleting procedures will prevent them from being called.
        ```bash
        bq rm -f -r project.dataset.r_ausd_vertrag_control
        bq rm -f -r project.dataset.starte_sql_skript
        bq rm -f -r project.dataset.d_ausd_v_ta_apn_ve
        ```
3.  **Data Rollback (if necessary):**
    *   If the `project.dataset.target_table_for_ta_apn_ve` was modified incorrectly, use BigQuery's time travel capability to restore the table to a state before the migration:
        ```bash
        bq cp project.dataset.target_table_for_ta_apn_ve@$(date -d '1 hour ago' +%s000) project.dataset.target_table_for_ta_apn_ve_restored
        # Then, if confident, replace the original table
        bq cp -f project.dataset.target_table_for_ta_apn_ve_restored project.dataset.target_table_for_ta_apn_ve
        ```
    *   Alternatively, restore from any existing backups of the target table.
    *   The `error_log`, `job_table`, and `job_run_audit` tables are for logging and state; typically, their data would not need to be rolled back, but could be cleared if desired.
4.  **Re-enable Original Job:**
    *   Reactivate the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_apn_ve.ksh` script in its original scheduling system (e.g., cron).
    *   Ensure all necessary environment variables and dependencies for the original script are correctly configured and accessible.