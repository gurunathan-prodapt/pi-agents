# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the `k_ausd_bp_ta_bpr_optionen.ksh` job. This job, originally a KornShell script orchestrating an Oracle SQL script (`d_ausd_bp_ta_bpr_optionen.sql`), is responsible for preparing and populating data related to "Basisprodukt" (base product) tariff options. It extracts or derives data from `sof$ta_bpr_instance` and `isbert_schema.dwtk_meldungen`, and loads it into `sof$ta_bpr_optionen` after truncation.

The job has been migrated from an on-premises Oracle/KornShell environment to Google Cloud Platform. The core data processing logic is now implemented as a BigQuery SQL Stored Procedure, leveraging BigQuery for data storage and execution. The orchestration layer, previously handled by the KornShell script, is replaced by a Python script designed for execution within Google Cloud Composer (Apache Airflow) or Google Cloud Workflows.

## 2. Generated Artifacts

The migration produced the following artifacts:

*   **`bigquery/ddl/isbert_schema/dwtk_meldungen.sql`**
    *   **Role:** BigQuery DDL for the `dwtk_meldungen` table within the `isbert_schema` dataset. This table serves as a source for date derivation, mirroring the original Oracle table's role.
*   **`bigquery/ddl/sof_ta_bpr_instance.sql`**
    *   **Role:** BigQuery DDL for the `sof_ta_bpr_instance` table. This table acts as the primary source for the base product instance data, replicating the original Oracle `sof$ta_bpr_instance` table.
*   **`bigquery/ddl/sof_ta_bpr_optionen.sql`**
    *   **Role:** BigQuery DDL for the `sof_ta_bpr_optionen` table. This is the target table where the processed base product tariff options are loaded, replacing the original Oracle `sof$ta_bpr_optionen` table.
*   **`bigquery/ddl/error_log.sql`**
    *   **Role:** BigQuery DDL for a centralized `error_log` table. This table captures error details from the BigQuery Stored Procedure, replacing the shell script's file-based error logging.
*   **`bigquery/ddl/job_log.sql`**
    *   **Role:** BigQuery DDL for a centralized `job_log` table. This table records details of each job execution, including processed record counts, replacing the shell script's temporary files and implicit logging.
*   **`bigquery/stored_procedures/sp_d_ausd_bp_ta_bpr_optionen.sql`**
    *   **Role:** BigQuery SQL Stored Procedure. This is the core component that encapsulates the combined logic of the original `k_ausd_bp_ta_bpr_optionen.ksh` (parameter handling, date validation, environment setup) and `d_ausd_bp_ta_bpr_optionen.sql` (data truncation, insertion, and logging). It takes input parameters, performs data transformations, and logs execution details.
*   **`python/orchestration/invoke_sp_d_ausd_bp_ta_bpr_optionen.py`**
    *   **Role:** Python script designed to be run by an orchestration service (e.g., Cloud Composer/Airflow, Cloud Workflows). It parses command-line arguments, constructs the BigQuery Stored Procedure call, and executes it. This script replaces the orchestration and parameter passing responsibilities of the original KornShell script.

## 3. Key Design Decisions

The following key design decisions guided the migration:

*   **Consolidation into BigQuery Stored Procedure:** The logic from both the KornShell orchestrator and the Oracle SQL script was combined into a single BigQuery Stored Procedure (`sp_d_ausd_bp_ta_bpr_optionen`). This centralizes the business logic, leverages BigQuery's native performance for SQL operations, and simplifies deployment and maintenance compared to managing separate shell and SQL files.
*   **Python-based Orchestration:** A Python script was chosen to replace the KornShell script's orchestration role. This aligns with modern cloud-native practices, provides better integration with Google Cloud services (like BigQuery client libraries), and is suitable for scheduling via Cloud Composer (Airflow) or Cloud Workflows.
*   **Dedicated BigQuery Logging Tables:** Instead of relying on file-based logging or temporary files as in the original shell script, dedicated `error_log` and `job_log` tables were created in BigQuery. This provides centralized, queryable, and persistent logging for all migrated jobs, improving observability and troubleshooting.
*   **Direct BigQuery SQL for Operations:** Oracle-specific constructs like `isbert_schema.DWPA_UTIL_SKRIPT.runstatement` for `TRUNCATE TABLE` and performance hints (`/*+ full(bp) parallel(bp,4) */`) were replaced with their direct BigQuery Standard SQL equivalents. BigQuery's query optimizer automatically handles execution plans, making explicit hints unnecessary.
*   **Parameter and Date Validation within Stored Procedure:** The validation logic for input parameters and date formats, originally handled by shell utilities (`h_alis_date.ksh`, `h_alis_parameter.ksh`), was integrated directly into the BigQuery Stored Procedure. This ensures that validation occurs close to the data processing logic and allows for consistent error logging.
*   **Trade-offs:**
    *   **Increased Dependency on BigQuery:** The solution is now tightly coupled with BigQuery, requiring familiarity with BigQuery SQL and its ecosystem.
    *   **Initial Data Ingestion:** Source tables (`dwtk_meldungen`, `sof_ta_bpr_instance`) require a separate ingestion process to populate them in BigQuery before the stored procedure can run.
    *   **Semantic Differences:** While largely compatible, subtle differences between Oracle SQL and BigQuery Standard SQL (e.g., date functions, data type handling) required careful translation and testing.
    *   **Loss of Shell Script Flexibility:** The ability to perform arbitrary file system operations or complex external command chaining, inherent in shell scripts, is replaced by structured BigQuery SQL and Python, which might require different approaches for certain edge cases (though none were apparent in this specific job).

## 4. Manual Steps Before Go-Live

Before the migrated job can be put into production, the following manual steps must be completed:

1.  **GCP Project Setup:**
    *   Ensure a Google Cloud Project is active and configured.
    *   Enable the BigQuery API.

2.  **BigQuery Dataset Creation:**
    *   Create the BigQuery dataset for `isbert_schema`:
        ```bash
        bq mk --dataset --default_table_expiration 365 `your-gcp-project-id:your-isbert-schema-dataset`
        ```
    *   Create the BigQuery dataset for the main job tables and logs:
        ```bash
        bq mk --dataset --default_table_expiration 365 `your-gcp-project-id:your-dataset`
        ```
    *   **Note:** Replace `your-gcp-project-id`, `your-isbert-schema-dataset`, and `your-dataset` with your actual project ID and desired dataset names.

3.  **BigQuery Table DDL Execution:**
    *   Execute the DDL scripts to create the necessary tables. **Crucially, review and adjust data types and sizes in these DDLs based on the actual Oracle source schema.** The provided DDLs are best-effort inferences.
        *   `bigquery/ddl/isbert_schema/dwtk_meldungen.sql`
        *   `bigquery/ddl/sof_ta_bpr_instance.sql`
        *   `bigquery/ddl/sof_ta_bpr_optionen.sql`
        *   `bigquery/ddl/error_log.sql`
        *   `bigquery/ddl/job_log.sql`
    *   Example for `dwtk_meldungen.sql`:
        ```bash
        bq query --use_legacy_sql=false < bigquery/ddl/isbert_schema/dwtk_meldungen.sql
        ```
        (Repeat for all DDL files)

4.  **Initial Data Loading:**
    *   Ingest historical and/or initial data from the Oracle source system into the newly created BigQuery tables:
        *   `your-gcp-project-id.your-isbert-schema-dataset.dwtk_meldungen`
        *   `your-gcp-project-id.your-dataset.sof_ta_bpr_instance`
    *   This can be done using various GCP tools like Cloud Data Fusion, Dataflow, BigQuery Data Transfer Service, or custom scripts. Ensure data consistency and completeness.

5.  **BigQuery Stored Procedure Deployment:**
    *   Execute the `bigquery/stored_procedures/sp_d_ausd_bp_ta_bpr_optionen.sql` script to create the stored procedure in BigQuery.
        ```bash
        bq query --use_legacy_sql=false < bigquery/stored_procedures/sp_d_ausd_bp_ta_bpr_optionen.sql
        ```

6.  **IAM & Permissions:**
    *   Create a dedicated GCP Service Account for the orchestration layer (e.g., Cloud Composer, Cloud Workflows).
    *   Grant this Service Account the following BigQuery roles:
        *   `BigQuery Data Editor` (to write to `sof_ta_bpr_optionen`, `error_log`, `job_log`)
        *   `BigQuery Data Viewer` (to read from `dwtk_meldungen`, `sof_ta_bpr_instance`)
        *   `BigQuery Job User` (to run queries and stored procedures)
    *   If using Cloud Composer, ensure the Composer environment's service account has these permissions.

7.  **Orchestration Setup (e.g., Cloud Composer/Airflow):**
    *   Upload the `python/orchestration/invoke_sp_d_ausd_bp_ta_bpr_optionen.py` script to your Cloud Composer DAGs folder or configure a Cloud Workflow definition.
    *   Create an Airflow DAG or Workflow definition to schedule and execute this Python script.
    *   Configure the DAG/Workflow to pass the required parameters (`job_kennung`, `eintrags_nr`, `stichtag`, `wiederanlauf_wert`, `project_id`, `dataset_id`, `isbert_schema_dataset_id`) to the Python script.
    *   Ensure the Python script has access to the `google-cloud-bigquery` library.

## 5. Known Gaps & Unresolved References

The following items are flagged for follow-up or represent areas of potential risk:

*   **Missing Complexity and Automation Rate Metrics:** The original design document lacked `file_complexity` and `automation_rate` metrics. This means the initial effort estimation and the assumed automation level were based on general assumptions. A manual review of the script's actual complexity and a more precise assessment of its automation potential are recommended for future migrations.
*   **`starteSQLSkript` Implementation Details:** The exact implementation of the `starteSQLSkript` helper function within `h_alis_sqlplus.ksh` was not fully known. The migration assumes it was a straightforward wrapper for executing SQL via SQL*Plus. If it contained complex dynamic SQL generation or other intricate logic, this might require further investigation and potential adjustments to the BigQuery Stored Procedure.
*   **Oracle Data Types and Constraints:** The migration assumed a direct and compatible mapping of Oracle data types to BigQuery data types (e.g., `NUMBER` to `INT64`, `DATE` to `TIMESTAMP`/`DATE`). Specific handling for `NUMBER` precision/scale, `VARCHAR2` lengths, and other Oracle-specific constraints (e.g., `NOT NULL` behavior, default values) should be thoroughly verified against the actual Oracle schema.
*   **Data Volume and Performance:** The presence of Oracle hints like `/*+ full(bp) parallel(bp,4) */` in the original SQL suggests potentially large data volumes. While BigQuery is designed for scale, the performance of the migrated solution should be closely monitored post-migration. Optimization strategies such as partitioning, clustering, or appropriate indexing might be required if performance bottlenecks are observed.
*   **Commented-out Code:** The original KornShell script contained commented-out references to `FOSJobDeaktivate`, `FOSJobErzeugeEintrag`, and shell commands like `sed`, `sort`, `join`. These functionalities were ignored during the migration as they were inactive. If these become active requirements in the future, they represent additional scope and design effort, potentially requiring BigQuery data manipulation, external tables, or integration with services like Dataflow for complex file processing.
*   **`v_carmen` Variable:** The original SQL script declared `v_carmen` but it was not used. This variable was omitted from the BigQuery Stored Procedure. If it has a hidden purpose, it needs to be re-evaluated.

## 6. Validation

To ensure the successful migration and correct operation of the job, the following validation steps should be performed:

1.  **BigQuery Stored Procedure Unit Testing:**
    *   Manually execute the `sp_d_ausd_bp_ta_bpr_optionen` stored procedure in BigQuery with various valid and invalid parameter combinations.
    *   **Passing Criteria:**
        *   For valid inputs, the procedure should complete successfully without errors.
        *   For invalid inputs (e.g., missing `p_JobKennung`, invalid `p_Stichtag` format), the procedure should raise an error and log an entry to `your-gcp-project-id.your-dataset.error_log`.
        *   Verify that `your-gcp-project-id.your-dataset.sof_ta_bpr_optionen` is truncated and then populated with data.
        *   Verify that an entry is correctly inserted into `your-gcp-project-id.your-dataset.job_log` with accurate `records` count and parameter values.

2.  **Orchestration Integration Testing:**
    *   Execute the `python/orchestration/invoke_sp_d_ausd_bp_ta_bpr_optionen.py` script locally or within a test Cloud Composer/Workflows environment.
    *   **Passing Criteria:**
        *   The Python script should execute successfully, invoking the BigQuery Stored Procedure.
        *   The BigQuery Stored Procedure should complete successfully as per the unit test criteria above.
        *   Verify that the `stichtag` parameter is correctly defaulted to yesterday's date if not provided.
        *   Monitor logs in Cloud Logging for any errors from the Python script or BigQuery.

3.  **Data Validation and Reconciliation:**
    *   Run the original Oracle job and the migrated BigQuery job with the same input parameters and source data state.
    *   **Passing Criteria:**
        *   The record count in the target table `sof$ta_bpr_optionen` (Oracle) should match the `records` count logged in `your-gcp-project-id.your-dataset.job_log` for `sof_ta_bpr_optionen` (BigQuery).
        *   Perform a row-by-row comparison (e.g., using checksums or `EXCEPT` queries) between the data in `sof$ta_bpr_optionen` in Oracle and `your-gcp-project-id.your-dataset.sof_ta_bpr_optionen` in BigQuery. The data should be identical.

4.  **Performance Testing:**
    *   Monitor the execution time and resource consumption of the BigQuery Stored Procedure and the orchestration layer.
    *   **Passing Criteria:** The job should complete within acceptable performance SLAs.

## 7. Rollback Procedure

In the event of critical issues or unexpected behavior after go-live, the following rollback procedure can be initiated:

1.  **Stop New Migrated Job Runs:**
    *   Immediately disable or pause the Cloud Composer DAG or Cloud Workflow that invokes the `invoke_sp_d_ausd_bp_ta_bpr_optionen.py` script. This prevents any further execution of the migrated job.

2.  **Revert Scheduling to Original System:**
    *   Re-enable or resume the original scheduling mechanism for the `k_ausd_bp_ta_bpr_optionen.ksh` script in the on-premises environment.

3.  **Data Restoration (if necessary):**
    *   If the `your-gcp-project-id.your-dataset.sof_ta_bpr_optionen` table in BigQuery was corrupted or incorrectly populated, and this corruption propagated to downstream systems, restore the `sof$ta_bpr_optionen` table in the Oracle environment from the last known good backup.
    *   If the BigQuery target table itself needs to be reverted, it can be restored using BigQuery's time travel feature (up to 7 days by default) or from a snapshot/backup if configured.

4.  **Cleanup (Optional, post-rollback):**
    *   Once the original system is stable and operational, the BigQuery objects (stored procedure, tables, datasets) and the orchestration components (Python script, DAG/Workflow) related to this migration can be disabled or deleted. This step should only be performed after a confirmed successful rollback and a decision to halt the migration effort.