# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `k_ausd_v_ta_disc_zusgf.ksh` and its associated SQL logic (`d_ausd_v_ta_disc_zusgf.sql`). The original script served as an orchestration layer, managing job execution, parameter handling, and invoking an underlying SQL script to update the `ta_disc_zusgf` table.

The migration targets the Google Cloud Platform (GCP), specifically:
*   **BigQuery**: For data storage, processing, and encapsulating the core SQL logic and orchestration within Stored Procedures.
*   **Cloud Composer (Apache Airflow)**: For scheduling, monitoring, and orchestrating the end-to-end workflow, replacing the KornShell script's control flow.

The primary goal was to re-implement the existing functionality in a cloud-native, scalable, and maintainable manner.

## 2. Generated Artifacts

The migration produced the following files, replacing the original KornShell script and its dependencies:

*   **`sql/ddl/job_error_log.sql`**
    *   **Role**: BigQuery Data Definition Language (DDL) script to create the `job_error_log` table. This table captures details of any errors encountered during job execution, replacing the ad-hoc error logging mechanisms of the original KornShell script.
*   **`sql/ddl/job_run_log.sql`**
    *   **Role**: BigQuery DDL script to create the `job_run_log` table. This table records successful job executions, including parameters and the number of records processed, replacing the temporary file (`.tmp`) mechanism used by the original script to store record counts.
*   **`sql/ddl/ta_disc_zusgf.sql`**
    *   **Role**: BigQuery DDL script to create the `ta_disc_zusgf` table. This is the target table for the data processing, migrated from its original Oracle (implied by `sof$`) schema to BigQuery.
*   **`sql/ddl/ta_discount.sql`**
    *   **Role**: BigQuery DDL script to create the `ta_discount` table. This is a source table used by the core SQL logic, migrated from its original Oracle (implied by `sof$`) schema to BigQuery.
*   **`sql/ddl/dwtk_meldungen.sql`**
    *   **Role**: BigQuery DDL script to create the `dwtk_meldungen` table. This is another source table, specifically used to derive `v_datum` in the original SQL, migrated from its original Oracle schema to BigQuery.
*   **`sql/stored_procedures/d_ausd_v_ta_disc_zusgf_sp.sql`**
    *   **Role**: BigQuery Stored Procedure that encapsulates the core data manipulation logic previously found in `d_ausd_v_ta_disc_zusgf.sql`. It performs the `TRUNCATE` and `INSERT` operations into `ta_disc_zusgf`, including the translation of Oracle-specific functions like `TABLE(sof$sp_discount_functions.concat_discounts(CURSOR(...)))` into BigQuery SQL. It also returns the count of processed records.
*   **`sql/stored_procedures/r_ausd_vertrag_control.sql`**
    *   **Role**: BigQuery Stored Procedure that serves as the primary orchestration and control layer, directly replacing the `k_ausd_v_ta_disc_zusgf.ksh` script. It handles parameter validation, calls `d_ausd_v_ta_disc_zusgf_sp`, and logs job status and errors to the `job_run_log` and `job_error_log` tables, respectively.
*   **`python/dags/k_ausd_v_ta_disc_zusgf_dag.py`**
    *   **Role**: An Apache Airflow DAG written in Python. This DAG is responsible for scheduling and executing the `r_ausd_vertrag_control` BigQuery Stored Procedure. It provides the external scheduling and parameter passing mechanism, replacing the cron-based or manual execution of the original KornShell script.

## 3. Key Design Decisions

*   **BigQuery Stored Procedures for Core Logic and Orchestration**:
    *   **Why**: Encapsulating both the core SQL logic (`d_ausd_v_ta_disc_zusgf_sp`) and the orchestration/control flow (`r_ausd_vertrag_control`) within BigQuery Stored Procedures offers several advantages:
        *   **Direct Translation**: Allows for a more direct and maintainable translation of the original script's control flow and SQL logic into a single, cohesive BigQuery environment.
        *   **Performance**: Leverages BigQuery's optimized execution engine for data processing.
        *   **Atomicity**: BigQuery DML operations are atomic, simplifying transaction management compared to external script orchestration.
        *   **Reduced Data Movement**: Keeps data processing within BigQuery, minimizing data egress and ingress costs/latency.
    *   **Notable Trade-offs**: While powerful, BigQuery Stored Procedures have limitations compared to full-fledged programming languages (e.g., Python) for complex logic or external system integrations. For this job, the logic was deemed suitable for BigQuery SQL scripting.

*   **Cloud Composer (Airflow) for External Orchestration**:
    *   **Why**: Airflow provides robust scheduling, dependency management, monitoring, and parameterization capabilities, making it an ideal replacement for the KornShell script's role as an external orchestrator. It allows for centralized management of workflows and integrates seamlessly with other GCP services.
    *   **Notable Trade-offs**: Introduces an additional layer of infrastructure (Cloud Composer environment) and requires Python development skills for DAG creation.

*   **Replacement of KornShell Utility Scripts**:
    *   **Why**: The functionalities of sourced KSH utility scripts (e.g., `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`) were absorbed directly into the BigQuery Stored Procedure (`r_ausd_vertrag_control`) using BigQuery SQL scripting features (e.g., `IF`, `RAISE`, `INSERT` into logging tables). This eliminates external dependencies and consolidates logic within BigQuery.

*   **Replacement of Temporary Files for Metrics**:
    *   **Why**: The original script used a temporary file (`.tmp`) to pass record counts. This was replaced by an `OUT` parameter in the `d_ausd_v_ta_disc_zusgf_sp` and `r_ausd_vertrag_control` procedures, and by logging the final count to the `job_run_log` BigQuery table. This provides a persistent, queryable, and more reliable mechanism for capturing job metrics.

*   **Handling Oracle-Specific SQL Features**:
    *   **`TRUNCATE TABLE`**: Directly translated to BigQuery's `TRUNCATE TABLE`.
    *   **`COMMIT` / `ANALYZE TABLE`**: Explicit `COMMIT` is not needed in BigQuery as DML operations are atomic. `ANALYZE TABLE` is not required as BigQuery automatically manages statistics.
    *   **`ALTER SESSION ENABLE PARALLEL DML` / `SET optimizer_dynamic_sampling`**: These Oracle-specific performance hints are not applicable to BigQuery and were removed.
    *   **`TABLE(sof$sp_discount_functions.concat_discounts(CURSOR(...)))`**: This complex Oracle pipelined table function was re-implemented using BigQuery's `STRING_AGG` and `GROUP BY` clauses to achieve similar concatenation logic, including handling the `SUBSTR` for length limits.

## 4. Manual Steps Before Go-Live

Before the migrated job can be put into production, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**:
    *   Ensure the target BigQuery dataset (`my_project.my_dataset` as per the generated code) exists. If not, create it in the GCP Console or using `bq mk`.
2.  **BigQuery Table Creation (DDL Execution)**:
    *   Execute the DDL scripts for all tables:
        *   `sql/ddl/job_error_log.sql`
        *   `sql/ddl/job_run_log.sql`
        *   `sql/ddl/ta_disc_zusgf.sql`
        *   `sql/ddl/ta_discount.sql`
        *   `sql/ddl/dwtk_meldungen.sql`
    *   This can be done via the BigQuery UI, `bq query`, or a CI/CD pipeline.
3.  **BigQuery Stored Procedure Deployment**:
    *   Execute the DDL scripts for the stored procedures:
        *   `sql/stored_procedures/d_ausd_v_ta_disc_zusgf_sp.sql`
        *   `sql/stored_procedures/r_ausd_vertrag_control.sql`
    *   These must be deployed to the target BigQuery dataset.
4.  **Data Migration**:
    *   Migrate the historical and initial data for the source tables (`ta_discount`, `dwtk_meldungen`) and the target table (`ta_disc_zusgf`) from the legacy Oracle environment to their respective BigQuery tables. This can be done using various GCP tools like BigQuery Data Transfer Service, `bq load` from Cloud Storage, or custom data pipelines.
5.  **IAM Permissions**:
    *   Ensure the Service Account used by your Cloud Composer environment (or the user running the Airflow DAG) has the necessary BigQuery permissions:
        *   `BigQuery Data Editor` on `my_project.my_dataset` (to write to `ta_disc_zusgf`, `job_error_log`, `job_run_log`).
        *   `BigQuery Data Viewer` on `my_project.my_dataset` (to read from `ta_discount`, `dwtk_meldungen`).
        *   `BigQuery Job User` (to run BigQuery jobs, including stored procedures).
6.  **Airflow Environment Configuration**:
    *   **GCP Connection**: Verify that the `google_cloud_default` connection is configured correctly in your Airflow environment and points to the appropriate GCP project.
    *   **DAG Deployment**: Upload the `python/dags/k_ausd_v_ta_disc_zusgf_dag.py` file to your Cloud Composer environment's DAGs folder.
    *   **DAG Parameters**: Review and set appropriate default values for `PROJECT_ID` and `DATASET_ID` within the DAG file, or ensure they are correctly configured via Airflow Variables or Connections.
    *   **Scheduling**: Define the desired `schedule_interval` for the `k_ausd_v_ta_disc_zusgf_dag` in the DAG file.
7.  **Review `JobKennung` and `EintragsNr`**:
    *   Ensure the default values or expected input mechanisms for `job_kennung` and `eintrags_nr` parameters in the Airflow DAG are appropriate for your environment.

## 5. Known Gaps & Unresolved References

*   **Job Status Management (Active Jobs)**: The original design document mentioned "ignore active jobs" and "deactivate old active jobs." While the migrated solution logs job runs and errors, it does not explicitly re-implement a mechanism to check for or manage "active" job instances to prevent concurrent runs or clean up stale ones. Airflow's built-in scheduler and task concurrency settings can mitigate some of this, but if the original script had a more complex, custom job control system, this aspect might require further design and implementation (e.g., using a BigQuery control table for job locks).
*   **`v_datum` Variable Usage**: In the original `d_ausd_v_ta_disc_zusgf.sql`, a `v_datum` variable was defined but not directly used in the main `INSERT` statement. The migrated BigQuery Stored Procedure `d_ausd_v_ta_disc_zusgf_sp` omits this variable. If future logic or a deeper analysis reveals a hidden dependency on `v_datum`, it would need to be re-introduced.
*   **Helper Script Logic Granularity**: While the core functionalities of the KSH helper scripts (error handling, parameter parsing) have been absorbed, any very specific or nuanced logic within those scripts (e.g., complex date calculations from `h_alis_date.ksh` beyond what's directly used) might need further review if issues arise. The current implementation assumes the primary functions were parameter validation and error messaging.

## 6. Validation

To validate the migrated job, perform the following steps:

1.  **Manual Execution of BigQuery Stored Procedures**:
    *   **Test `d_ausd_v_ta_disc_zusgf_sp`**: Execute `CALL my_project.my_dataset.d_ausd_v_ta_disc_zusgf_sp('TEST_EINTRAG', 'TEST_JOB', 'ta_disc_zusgf', @records_processed);` directly in BigQuery.
        *   **Passing Criteria**: The procedure should complete successfully, `ta_disc_zusgf` should be populated, and `@records_processed` should reflect the correct number of inserted rows.
    *   **Test `r_ausd_vertrag_control` (Success Path)**: Execute `CALL my_project.my_dataset.r_ausd_vertrag_control('TEST_JOB_SUCCESS', 'TEST_ENTRY_SUCCESS', @total_records);`
        *   **Passing Criteria**: The procedure should complete successfully. An entry should appear in `my_project.my_dataset.job_run_log` with `job_kennung='TEST_JOB_SUCCESS'`, `eintrags_nr='TEST_ENTRY_SUCCESS'`, and a non-zero `records_processed` count. The `ta_disc_zusgf` table should be updated as expected.
    *   **Test `r_ausd_vertrag_control` (Error Path - Missing JobKennung)**: Execute `CALL my_project.my_dataset.r_ausd_vertrag_control(NULL, 'TEST_ENTRY_ERROR', @total_records);`
        *   **Passing Criteria**: The procedure should raise an exception (e.g., `FEHLER: 0 E 193 Jobkennung`). An entry should appear in `my_project.my_dataset.job_error_log` with `job_kennung=NULL`, `eintrags_nr='TEST_ENTRY_ERROR'`, `err_nr=193`, and `err_arg='Jobkennung'`.
    *   **Test `r_ausd_vertrag_control` (Error Path - Missing EintragsNr)**: Execute `CALL my_project.my_dataset.r_ausd_vertrag_control('TEST_JOB_ERROR', NULL, @total_records);`
        *   **Passing Criteria**: The procedure should raise an exception (e.g., `FEHLER: 0 E 193 EintragsNr`). An entry should appear in `my_project.my_dataset.job_error_log` with `job_kennung='TEST_JOB_ERROR'`, `eintrags_nr=NULL`, `err_nr=193`, and `err_arg='EintragsNr'`.

2.  **Airflow DAG Execution**:
    *   **Trigger DAG Manually**: In the Airflow UI, trigger the `k_ausd_v_ta_disc_zusgf_dag` manually, providing sample `job_kennung` and `eintrags_nr` parameters.
        *   **Passing Criteria**: The DAG run should complete successfully (green status). The `call_r_ausd_vertrag_control` task should succeed. Verify that a corresponding entry exists in `my_project.my_dataset.job_run_log` with the provided parameters and a valid `records_processed` count.
    *   **Trigger DAG with Invalid Parameters (Simulated)**: If possible, configure the DAG to pass `NULL` or empty strings for `job_kennung` or `eintrags_nr` (e.g., by modifying the DAG temporarily or using a separate test DAG).
        *   **Passing Criteria**: The `call_r_ausd_vertrag_control` task should fail. An entry should appear in `my_project.my_dataset.job_error_log` reflecting the parameter validation error.
    *   **Data Comparison**: After successful runs, compare the data in the `my_project.my_dataset.ta_disc_zusgf` table with the expected output from the legacy system for the same input parameters. This is the ultimate validation of the business logic.

## 7. Rollback Procedure

In case of critical issues or unexpected behavior after go-live, the following rollback procedure can be executed:

1.  **Deactivate Airflow DAG**:
    *   In the Airflow UI, toggle off the `k_ausd_v_ta_disc_zusgf_dag` to prevent any further scheduled or manual executions of the migrated job.
2.  **Revert to Legacy Execution**:
    *   Re-enable the original `k_ausd_v_ta_disc_zusgf.ksh` script in its legacy scheduling environment (e.g., cron, scheduler).
3.  **Data Restoration (if necessary)**:
    *   If the `ta_disc_zusgf` table in BigQuery was corrupted or incorrectly updated by the migrated job, restore it from the most recent valid backup. BigQuery's time travel feature can also be used to revert the table to a previous state if the corruption was recent and within the time travel window.
    *   For source tables (`ta_discount`, `dwtk_meldungen`), if they were affected by any unforeseen side effects (unlikely for this job as it only reads from them), they would also need restoration.
4.  **Investigate and Remediate**:
    *   Analyze the `job_error_log` and Cloud Logging entries for the failed BigQuery Stored Procedure calls or Airflow DAG runs to identify the root cause of the issue.
    *   Address the identified problems in the BigQuery Stored Procedures or the Airflow DAG.
5.  **Re-validate and Re-deploy**:
    *   Once the issues are resolved, follow the "Validation" steps again in a staging environment.
    *   After successful validation, re-deploy the corrected artifacts and proceed with a new go-live.