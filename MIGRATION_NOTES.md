# MIGRATION_NOTES.md

## 1. Summary

The KornShell orchestration script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_instance.ksh` has been migrated to Google Cloud Platform. This script, responsible for orchestrating the initial provisioning of selected basic products for BERT and generating a snapshot of contract cache data for "Forderungsscoring," now leverages BigQuery for data processing and storage, and Cloud Composer (Airflow) for robust orchestration.

## 2. Generated Artifacts

The migration process generated the following artifacts:

*   **`sql/ddl/job_log.sql`**
    *   **Role**: This SQL script defines the Data Definition Language (DDL) for the `job_log` table in BigQuery. This table serves as a centralized, structured repository for logging the execution status, parameters, and error details of the migrated job, replacing the original script's dynamic log files.
*   **`sql/sp/k_ausd_bp_ta_bpr_instance_sql.sql`**
    *   **Role**: This BigQuery Stored Procedure (`project.dataset.k_ausd_bp_ta_bpr_instance_sql`) is the migrated equivalent of the original `k_ausd_bp_ta_bpr_instance.ksh` kernel script. It is designed to encapsulate the core data extraction, transformation, and loading logic, operating directly within BigQuery. Currently, it contains placeholder logic, awaiting full implementation of the original kernel's business rules.
*   **`sql/sp/ausd_bp_ta_bpr_instance.sql`**
    *   **Role**: This BigQuery Stored Procedure (`project.dataset.ausd_bp_ta_bpr_instance`) is the migrated equivalent of the main `r_ausd_bp_ta_bpr_instance.ksh` orchestration script. It handles parameter parsing, date determination, logging of job status, and orchestrates the execution of the `k_ausd_bp_ta_bpr_instance_sql` kernel procedure. It includes error handling and status reporting to the `job_log` table.
*   **`dags/r_ausd_bp_ta_bpr_instance_dag.py`**
    *   **Role**: This Python script defines an Apache Airflow DAG (`r_ausd_bp_ta_bpr_instance_dag`) for Cloud Composer. Its purpose is to schedule and orchestrate the execution of the `project.dataset.ausd_bp_ta_bpr_instance` BigQuery Stored Procedure. It allows for passing parameters like `stichtag_str` and `wiederanlaufwert` to the BigQuery procedure and integrates with Airflow's monitoring and scheduling capabilities.

## 3. Key Design Decisions

*   **Orchestration to BigQuery Stored Procedure**: The primary `r_ausd_bp_ta_bpr_instance.ksh` orchestration script was migrated into a BigQuery Stored Procedure (`ausd_bp_ta_bpr_instance`). This centralizes the job's control flow, parameter handling, and logging directly within the data platform, leveraging BigQuery's native capabilities for procedural logic and error handling.
*   **Kernel Logic to BigQuery Stored Procedure**: The core data processing logic from `k_ausd_bp_ta_bpr_instance.ksh` was also migrated into a separate BigQuery Stored Procedure (`k_ausd_bp_ta_bpr_instance_sql`). This keeps the data transformation close to the data, optimizing performance and simplifying data lineage within BigQuery.
*   **Cloud Composer (Airflow) for Scheduling**: The legacy shell-based scheduling mechanism was replaced by Cloud Composer (Airflow). This provides a robust, scalable, and observable orchestration platform with built-in features for scheduling, dependency management, retries, and monitoring.
*   **Centralized Structured Logging**: The original script's dynamic log file generation was replaced by inserts into a dedicated `job_log` BigQuery table. This enables structured logging, easier querying of job history, and integration with GCP's logging and monitoring services.
*   **Parameter Management**: Command-line parameters from the original script are now handled as input parameters for the BigQuery Stored Procedures and as DAG parameters in Airflow, providing clear interfaces and default handling.
*   **Error Handling Modernization**: The KornShell `trap` mechanisms were replaced by BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks for in-procedure error handling and by Airflow's task failure mechanisms for overall job failure management.
*   **Utility Script Integration**: Functionality from sourced utility scripts (e.g., `h_alis_date.ksh`, `h_alis_parameter.ksh`, `f_alis_msgerr.ksh`) was either integrated directly into the BigQuery Stored Procedures using native BigQuery functions (e.g., `PARSE_DATE`, `FORMAT_DATE`) or re-implemented as part of the procedure's logic.

**Notable Trade-offs:**

*   **Procedural Logic in SQL**: While BigQuery scripting allows for procedural logic, translating complex shell script logic into SQL can sometimes be less intuitive than traditional scripting languages.
*   **Loss of Direct Filesystem Access**: The ability to write arbitrary log files to a filesystem is replaced by structured logging to a database table, which is generally a positive change but requires adapting existing processes that might rely on file-based logs.
*   **Dependency on BigQuery**: The solution is now tightly coupled with BigQuery's ecosystem, which is beneficial for data processing but means less portability to other database systems.

## 4. Manual Steps Before Go-Live

Before the migrated job can be put into production, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**:
    *   Ensure the target BigQuery dataset (`project.dataset` as referenced in the generated code) exists. If not, create it in your GCP project.
2.  **DDL Execution for `job_log`**:
    *   Execute the `sql/ddl/job_log.sql` script in BigQuery to create the `job_log` table. This table is crucial for monitoring and auditing job executions.
3.  **Target Data Table Provisioning**:
    *   Ensure that all target data tables (e.g., `fos_tabelle`) and source data tables (e.g., `dwh_contract_cache`) referenced by the kernel logic (`k_ausd_bp_ta_bpr_instance_sql`) are correctly defined and accessible in BigQuery.
4.  **IAM Permissions Configuration**:
    *   **Cloud Composer Service Account**: The service account used by your Cloud Composer environment must have the necessary BigQuery permissions:
        *   `BigQuery Data Editor` or `BigQuery User` to execute stored procedures and insert data into the `job_log` table.
        *   Permissions to read from source tables and write to target tables as required by the kernel logic.
    *   **BigQuery Service Account (if applicable)**: If the BigQuery stored procedures are executed by a separate service account (e.g., for direct calls), ensure it has similar permissions.
5.  **Deploy BigQuery Stored Procedures**:
    *   Execute `sql/sp/k_ausd_bp_ta_bpr_instance_sql.sql` to create the kernel stored procedure.
    *   Execute `sql/sp/ausd_bp_ta_bpr_instance.sql` to create the main orchestration stored procedure.
6.  **Deploy Airflow DAG**:
    *   Upload the `dags/r_ausd_bp_ta_bpr_instance_dag.py` file to the DAGs folder of your Cloud Composer environment.
    *   Configure the `schedule` parameter within the DAG to match the desired execution frequency (e.g., `@daily`, `0 0 * * *`).
    *   Update `project_id` in the DAG to your actual GCP Project ID.
7.  **Configuration Management (if applicable)**:
    *   If `ProgVersion` or other configuration values are to be dynamic, ensure they are managed via Airflow Variables, a BigQuery configuration table, or another suitable GCP service.

## 5. Known Gaps & Unresolved References

The following items are identified as known gaps or require further follow-up, including potential redesign (B4) items:

*   **Kernel Script Logic Implementation (B4 Item)**: The `k_ausd_bp_ta_bpr_instance_sql.sql` stored procedure currently contains placeholder logic. The complete data transformation, filtering, and loading logic from the original `k_ausd_bp_ta_bpr_instance.ksh` must be thoroughly analyzed, translated into BigQuery SQL, and implemented. This is the most critical unresolved item.
*   **`DW_EintragsNr` Generation**: The `v_dw_eintrags_nr` in the main stored procedure is currently generated using a combination of timestamp and UUID. If the original `DW_EintragsNr` had specific business requirements (e.g., sequential, unique across systems, or derived from a specific source), this generation logic needs to be reviewed and potentially refined.
*   **`ProgVersion` Management**: The `v_program_version` is hardcoded as '1.0.0'. For production environments, this should ideally be managed dynamically, perhaps fetched from a configuration table, an Airflow Variable, or a CI/CD pipeline.
*   **External System Mapping**: While the design document mentions "FOS-Tabelle" and "DWH_VERTRAG_ID" as implicit dependencies, their exact BigQuery table names, schemas, and access patterns need to be explicitly confirmed and mapped during the kernel logic implementation.
*   **Complex Utility Script Logic**: While basic date and parameter handling are integrated, if the original `h_alis_parameter.ksh` or `f_alis_msgerr.ksh` contained highly complex, reusable functions, a deeper analysis might be needed to determine if they warrant dedicated BigQuery UDFs or more elaborate helper procedures.

## 6. Validation

Validation of the migrated job involves testing both the BigQuery stored procedures and the Airflow DAG.

**How to Run Tests:**

1.  **BigQuery Stored Procedure Unit Tests**:
    *   **`ausd_bp_ta_bpr_instance`**:
        *   Execute the procedure directly in BigQuery with various parameter combinations:
            *   `CALL project.dataset.ausd_bp_ta_bpr_instance(p_stichtag_str => '01012023', p_wiederanlaufwert => 0);`
            *   `CALL project.dataset.ausd_bp_ta_bpr_instance(p_stichtag_str => NULL, p_wiederanlaufwert => 0);` (tests default `stichtag`)
            *   `CALL project.dataset.ausd_bp_ta_bpr_instance(p_stichtag_str => '01012023', p_wiederanlaufwert => NULL);` (tests default `wiederanlaufwert`)
            *   `CALL project.dataset.ausd_bp_ta_bpr_instance(p_stichtag_str => 'INVALID_DATE', p_wiederanlaufwert => 0);` (tests error handling for invalid date)
        *   Query the `project.dataset.job_log` table after each execution to verify entries.
    *   **`k_ausd_bp_ta_bpr_instance_sql`**:
        *   Once the kernel logic is fully implemented, create a separate test harness or execute it directly with sample data and parameters to verify its data transformation output.
2.  **Airflow DAG Integration Tests**:
    *   In the Cloud Composer UI, navigate to the `r_ausd_bp_ta_bpr_instance_dag`.
    *   Manually trigger the DAG using the "Trigger DAG" button.
    *   Provide different values for the `stichtag_str` and `wiederanlaufwert` parameters in the trigger UI.
    *   Monitor the DAG run in the Airflow UI (Graph View, Gantt Chart, Logs) for successful task completion.
    *   After each DAG run, query the `project.dataset.job_log` table to confirm the job status and details.
    *   Verify the data in the target BigQuery tables (e.g., `fos_tabelle`) to ensure the kernel logic produced the expected output.

**What "Passing" Means:**

*   **Airflow DAG**: The `r_ausd_bp_ta_bpr_instance_dag` completes successfully in Airflow, with all tasks marked as "success."
*   **BigQuery `job_log`**: For each successful DAG run, there should be a corresponding entry in `project.dataset.job_log` with `status = 'SUCCEEDED'` and no `error_details`. For intentional error tests, `status = 'FAILED'` with appropriate `error_details` is expected.
*   **Data Integrity**: The data generated or updated in the target BigQuery tables (e.g., `fos_tabelle`) by the `k_ausd_bp_ta_bpr_instance_sql` procedure matches the expected output based on the original script's logic and sample input data. This is the most critical validation point for the kernel logic.
*   **Parameter Handling**: Defaulting logic for `stichtag` and `wiederanlaufwert` works as expected, and invalid inputs are gracefully handled and logged.

## 7. Rollback Procedure

In the event of critical issues or unexpected behavior after go-live, the following rollback procedure should be followed:

1.  **Immediate Action - Disable New Job**:
    *   **Pause Airflow DAG**: In the Cloud Composer UI, locate the `r_ausd_bp_ta_bpr_instance_dag` and toggle its status to "Off" (paused). This will prevent any further scheduled or manual runs of the migrated job.
    *   **Disable BigQuery Stored Procedures**: (Optional, but recommended for critical issues) Rename or revoke execution permissions from the `project.dataset.ausd_bp_ta_bpr_instance` and `project.dataset.k_ausd_bp_ta_bpr_instance_sql` procedures to ensure they cannot be accidentally called.
2.  **Re-enable Legacy Job**:
    *   Re-enable the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_instance.ksh` job in its legacy scheduling system (e.g., cron, Control-M).
    *   Verify that the legacy job can run successfully and produce the expected output.
3.  **Data Rollback (if necessary)**:
    *   **Identify Affected Data**: Determine which target tables and partitions in BigQuery were affected by the erroneous run of the migrated job.
    *   **BigQuery Time Travel**: If the data was written recently and within BigQuery's time travel window (7 days by default), use the `FOR SYSTEM_TIME AS OF` clause to query the table state before the erroneous run and potentially re-insert correct data or overwrite the incorrect data.
        ```sql
        -- Example to restore a table to a previous state (requires careful planning)
        CREATE OR REPLACE TABLE project.dataset.fos_tabelle AS
        SELECT * FROM project.dataset.fos_tabelle FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
        ```
    *   **Backup Restoration**: If time travel is not an option (e.g., outside the window, or complex changes), restore the affected tables from the most recent valid backup.
    *   **Idempotent Rerun**: If the original job is idempotent (i.e., can be run multiple times for the same date without adverse effects), simply re-run the legacy job for the affected processing date(s) to correct the data.
4.  **Code Rollback (Cleanup)**:
    *   **Remove Airflow DAG**: Delete the `r_ausd_bp_ta_bpr_instance_dag.py` file from the Cloud Composer DAGs folder.
    *   **Delete BigQuery Stored Procedures**: Drop the `project.dataset.ausd_bp_ta_bpr_instance` and `project.dataset.k_ausd_bp_ta_bpr_instance_sql` stored procedures from BigQuery.
    *   **Delete `job_log` Table**: If the `job_log` table was created solely for this migration and is no longer needed, it can be dropped.

After rollback, a root cause analysis should be performed to understand the failure and address the underlying issues before attempting re-migration.