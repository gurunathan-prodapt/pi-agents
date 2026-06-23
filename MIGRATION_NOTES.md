```markdown
# MIGRATION_NOTES.md: k_ausd_bp_ta_msisdn.ksh

## 1. Summary
This document details the migration of the KornShell script `k_ausd_bp_ta_msisdn.ksh` from a legacy on-premise environment to Google Cloud Platform (GCP). The original script orchestrated a data preparation job, including parameter validation, date derivation, execution of a core SQL script (`d_ausd_bp_ta_msisdn.sql`), and logging.

The migrated solution leverages:
*   **Google Cloud Composer (Apache Airflow)** for orchestration and scheduling.
*   **Google BigQuery** as the primary data warehouse and processing engine, replacing the shell script's logic and the underlying SQL*Plus execution. BigQuery Stored Procedures now encapsulate the orchestration and data transformation logic.
*   **BigQuery tables** for audit logging, error logging, and storing processed data.

## 2. Generated artifacts

The migration process generated the following files:

*   **`sql/ddl/job_error_log.sql`**
    *   **Role**: BigQuery DDL (Data Definition Language) script to create the `job_error_log` table. This table is used to capture and store detailed error messages and timestamps when the migrated job encounters issues during execution. It replaces the legacy error handling mechanisms.
*   **`sql/ddl/job_audit_log.sql`**
    *   **Role**: BigQuery DDL script to create the `job_audit_log` table. This table records audit information for each job run, including start/end times, status (SUCCESS/FAILED), job parameters, and the number of records processed. It replaces the functionality of the commented-out `FOSJobErzeugeEintrag` call in the original script.
*   **`sql/ddl/pool_basisprodukt.sql`**
    *   **Role**: BigQuery DDL script to create the `PoolBasisprodukt` table. This is a placeholder for the primary target table where the core data transformation logic (originally from `d_ausd_bp_ta_msisdn.sql`) will write its output. The schema is illustrative and needs to be finalized based on the actual `d_ausd_bp_ta_msisdn.sql` content.
*   **`sql/stored_procedures/d_ausd_bp_ta_msisdn.sql`**
    *   **Role**: BigQuery Stored Procedure. This procedure is intended to encapsulate the core data transformation logic that was originally present in `d_ausd_bp_ta_msisdn.sql`. It reads from source tables, applies transformations, and writes to target tables like `PoolBasisprodukt`. **Note**: This is currently a placeholder with dummy data insertion, as the original SQL content was not provided.
*   **`sql/stored_procedures/r_ausd_bp_ta_msisdn.sql`**
    *   **Role**: BigQuery Stored Procedure. This is the main orchestration procedure, replacing the `k_ausd_bp_ta_msisdn.ksh` script itself. It handles parameter validation, date derivation, calls the `d_ausd_bp_ta_msisdn` procedure, captures record counts, and logs audit/error information.
*   **`sql/stored_procedures/postprocess_cibasis.sql`**
    *   **Role**: BigQuery Stored Procedure. This is an optional placeholder procedure designed to re-implement the commented-out `sed`, `sort`, and `join` operations from the original ksh script, should they be deemed necessary. It would perform these operations directly on BigQuery tables.
*   **`dags/k_ausd_bp_ta_msisdn_dag.py`**
    *   **Role**: Apache Airflow DAG (Directed Acyclic Graph). This Python script defines the workflow for the migrated job. It orchestrates the execution of the BigQuery stored procedures, passes parameters, and manages dependencies. It is the entry point for scheduling and running the job on Cloud Composer.

## 3. Key design decisions

*   **BigQuery Stored Procedures for Logic Encapsulation**:
    *   **Decision**: The orchestration logic (from `k_ausd_bp_ta_msisdn.ksh`) and the core data transformation logic (from `d_ausd_bp_ta_msisdn.sql`) are migrated into separate BigQuery Stored Procedures (`r_ausd_bp_ta_msisdn` and `d_ausd_bp_ta_msisdn` respectively).
    *   **Rationale**: This centralizes the business logic within the data warehouse, leveraging BigQuery's powerful SQL engine for processing. It eliminates the need for external shell scripting or complex Python logic for data manipulation, simplifying maintenance and improving performance for large datasets.
    *   **Trade-offs**: Requires translation of shell-specific constructs (e.g., `getopts`, `if/then`, date commands) and SQL*Plus dialect to BigQuery SQL. Error handling and logging are also re-implemented using BigQuery's native features.
*   **Cloud Composer (Airflow) for Orchestration**:
    *   **Decision**: Apache Airflow on Cloud Composer is chosen to manage the execution flow, scheduling, and parameter passing for the BigQuery stored procedures.
    *   **Rationale**: Airflow provides robust scheduling, monitoring, and dependency management capabilities, which are superior to simple cron jobs or shell-based orchestration. It allows for clear visualization of workflows and easier debugging.
    *   **Trade-offs**: Introduces a new technology stack (Python, Airflow concepts) and requires setup and maintenance of a Composer environment.
*   **BigQuery Tables for Logging**:
    *   **Decision**: Dedicated BigQuery tables (`job_audit_log`, `job_error_log`) are used for capturing job execution details and errors.
    *   **Rationale**: Centralized logging within BigQuery allows for easy querying, analysis, and integration with other BigQuery-based reporting tools. It replaces disparate file-based logging and provides a structured, queryable history of job runs.
*   **Replacement of Temporary Files with BigQuery Constructs**:
    *   **Decision**: All temporary file operations (e.g., for record counts, intermediate data for `sed`/`sort`/`join`) are replaced by BigQuery temporary tables or direct variable assignments within stored procedures.
    *   **Rationale**: This eliminates I/O bottlenecks associated with filesystem operations, enhances security by keeping data within BigQuery, and simplifies the overall data flow by removing external file dependencies.
*   **Parameter Handling via Airflow DAG Runs**:
    *   **Decision**: Shell `getopts` parameter parsing is replaced by Airflow DAG run configurations, which are then passed as arguments to the BigQuery Stored Procedures.
    *   **Rationale**: This provides a standardized and auditable way to invoke jobs with specific parameters, integrating seamlessly with Airflow's UI and API.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **GCP Project and BigQuery Dataset Setup**:
    *   Ensure a GCP Project is available.
    *   Create a BigQuery Dataset (e.g., `your_bq_dataset`) in the target GCP Project where all tables and stored procedures will reside. Update `GCP_PROJECT_ID` and `BQ_DATASET_ID` placeholders in the generated code accordingly.
2.  **BigQuery DDL Execution**:
    *   Execute the DDL scripts to create the necessary tables:
        *   `sql/ddl/job_error_log.sql`
        *   `sql/ddl/job_audit_log.sql`
        *   `sql/ddl/pool_basisprodukt.sql` (Ensure the schema is finalized based on the actual `d_ausd_bp_ta_msisdn.sql` content).
    *   This can be done via the BigQuery UI, `bq` command-line tool, or a CI/CD pipeline.
3.  **BigQuery Stored Procedure Deployment**:
    *   Deploy the BigQuery Stored Procedures:
        *   `sql/stored_procedures/d_ausd_bp_ta_msisdn.sql` (Crucially, this needs to be populated with the actual translated logic from the original `d_ausd_bp_ta_msisdn.sql`).
        *   `sql/stored_procedures/r_ausd_bp_ta_msisdn.sql`
        *   `sql/stored_procedures/postprocess_cibasis.sql` (Only if the commented-out post-processing logic is required and implemented).
    *   These can be deployed using the BigQuery UI, `bq` command-line tool, or a CI/CD pipeline.
4.  **IAM Permissions**:
    *   Ensure the service account used by Cloud Composer (or the user running the DAG) has the following BigQuery roles:
        *   `BigQuery Data Editor` (to write to `job_audit_log`, `job_error_log`, `PoolBasisprodukt`, and any other target tables).
        *   `BigQuery Job User` (to run queries and stored procedures).
        *   `BigQuery Data Viewer` (to read from source tables).
5.  **Cloud Composer Environment Setup**:
    *   Ensure a Cloud Composer environment is provisioned and running.
    *   Configure the `google_cloud_default` Airflow connection to use the appropriate GCP Project and service account.
6.  **Airflow DAG Deployment**:
    *   Upload the `dags/k_ausd_bp_ta_msisdn_dag.py` file to the DAGs folder of your Cloud Composer environment.
7.  **Scheduling**:
    *   Review and set the `schedule_interval` in `dags/k_ausd_bp_ta_msisdn_dag.py` according to the required execution frequency (e.g., daily, hourly). If `None`, the DAG will only run manually or via external triggers.
8.  **Source Data Availability**:
    *   Ensure all source tables referenced by the `d_ausd_bp_ta_msisdn` BigQuery Stored Procedure are available in BigQuery and accessible by the Composer service account. This might involve separate data ingestion pipelines (e.g., Data Transfer Service, Cloud Storage, etc.).

## 5. Known gaps & unresolved references

*   **`d_ausd_bp_ta_msisdn.sql` content**: The most significant gap is the actual content of the original `d_ausd_bp_ta_msisdn.sql` script. The generated `d_ausd_bp_ta_msisdn.sql` BigQuery Stored Procedure is a placeholder. Its full translation and implementation are critical and represent the primary unknown and risk.
*   **Commented-out code decision**: The original script contained commented-out `sed`, `sort`, `join` operations. A decision needs to be made whether this logic is obsolete or if it represents an unexecuted but desired transformation that should be migrated. If needed, the `postprocess_cibasis.sql` stored procedure needs to be fully implemented and integrated into the Airflow DAG.
*   **Error Handling Details**: While a generic `job_error_log` table is provided, the specific error codes and messages from the legacy `f_alis_msgerr.ksh` helper script were not fully mapped. Further refinement might be needed to align with legacy error reporting if required.
*   **`DWMSG_MeldeFehler` and `FOSJobErzeugeEintrag`**: The exact implementation details of these legacy functions were not fully known. The migration assumes the provided `job_audit_log` and `job_error_log` tables adequately cover their functionality. Any specific business logic tied to these functions (e.g., sending alerts, triggering other processes) would need to be re-evaluated and implemented in Airflow or BigQuery.
*   **Locale/Encoding Issues**: The original script contained German comments. While BigQuery handles UTF-8 well, any string manipulations or comparisons involving non-ASCII characters in the `d_ausd_bp_ta_msisdn.sql` logic should be carefully reviewed for potential encoding issues during translation.
*   **`p_wiederanlauf_wert` parameter**: This parameter is included in the `r_ausd_bp_ta_msisdn` signature for completeness but is not explicitly used in the current migration logic. If it had specific restart/recovery functionality in the original script, that logic would need to be implemented.

## 6. Validation

To validate the migrated job, follow these steps:

1.  **Deploy all artifacts**: Ensure all DDLs, BigQuery Stored Procedures, and the Airflow DAG are deployed to their respective environments.
2.  **Trigger the Airflow DAG**:
    *   Navigate to the Airflow UI for your Cloud Composer environment.
    *   Find the `k_ausd_bp_ta_msisdn_orchestration_dag` DAG.
    *   Trigger a new DAG run, providing test parameters for `job_kennung`, `eintrags_nr`, and `stichtag` (in `DDMMYYYY` format, e.g., `31122023`).
3.  **Monitor DAG Execution**:
    *   Observe the DAG run in the Airflow UI. All tasks (`call_r_ausd_bp_ta_msisdn_sp` and optionally `call_postprocess_sp`) should complete successfully (green status).
    *   Check task logs for any errors or unexpected output.
4.  **Verify BigQuery Logs**:
    *   Query the `your_gcp_project.your_bq_dataset.job_audit_log` table. A new entry should exist for the triggered job run, with `status = 'SUCCESS'` and `records_processed` reflecting the number of records inserted by `d_ausd_bp_ta_msisdn`.
    *   If the DAG failed, check `your_gcp_project.your_bq_dataset.job_error_log` for detailed error messages.
5.  **Verify Target Data**:
    *   Query the `your_gcp_project.your_bq_dataset.PoolBasisprodukt` table (and any other target tables) to ensure data has been correctly inserted for the `p_stichtag` provided.
    *   Perform data quality checks:
        *   **Record Count**: Compare the `records_processed` in `job_audit_log` with the actual count in `PoolBasisprodukt` for the given `_processing_date` (or `stichtag`).
        *   **Data Integrity**: Spot-check a few records to ensure values, formats, and relationships are correct as per the original script's expected output.
        *   **Completeness**: Verify that all expected data for the given `stichtag` has been processed.
6.  **Passing Criteria**:
    *   The Airflow DAG completes successfully without errors.
    *   An entry with `status = 'SUCCESS'` is recorded in `job_audit_log`.
    *   No entries are found in `job_error_log` for the specific job run.
    *   The `PoolBasisprodukt` table (and any other target tables) contains the expected data, matching the record count and data quality standards of the original job.

## 7. Rollback procedure

In case of critical issues or failure of the migrated job, the following rollback procedure can be executed to revert to the legacy system:

1.  **Stop New Job Execution**:
    *   In the Airflow UI, pause the `k_ausd_bp_ta_msisdn_orchestration_dag` to prevent any further runs of the migrated job.
    *   If any runs are currently in progress, allow them to complete or manually mark them as failed, but do not trigger new ones.
2.  **Revert Data (if necessary)**:
    *   If the migrated job has written incorrect or partial data to `PoolBasisprodukt` or other target tables, identify the affected data (e.g., by `_processing_date` or `stichtag`).
    *   **Option A (Delete/Truncate)**: If the data is easily identifiable and can be safely removed without affecting other processes, delete or truncate the affected partitions/rows from the BigQuery target tables.
    *   **Option B (Restore from Backup)**: If BigQuery tables were backed up before the migration, restore the tables to their pre-migration state.
    *   **Option C (No Revert)**: If the migrated job only failed during initial processing and did not commit any incorrect data, or if the target tables are append-only and can tolerate duplicate/failed entries (which will be overwritten/corrected by the legacy system), no data revert might be necessary.
3.  **Restart Legacy Job**:
    *   Ensure the legacy `k_ausd_bp_ta_msisdn.ksh` script and its dependencies are fully operational.
    *   Restart the scheduling mechanism (e.g., cron job) for the legacy `k_ausd_bp_ta_msisdn.ksh` script.
    *   Verify that the legacy job runs successfully and produces the expected output in its original target systems.
4.  **Post-Rollback Analysis**:
    *   Analyze the root cause of the migration failure using Airflow logs, BigQuery error logs, and any other available diagnostics.
    *   Plan corrective actions before attempting re-migration.

**Note**: This rollback procedure assumes the legacy system remains fully functional and can be reactivated without significant effort. It is crucial to have a clear understanding of the impact of data changes made by the new system on downstream processes before attempting a rollback.
```