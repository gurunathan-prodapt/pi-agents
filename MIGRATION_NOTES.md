# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `k_ausd_bp_ta_apn_vertrag.ksh` and its associated SQL script `d_ausd_bp_ta_apn_vertrag.sql`. The original system orchestrated data processing related to APN contracts using shell scripting and Oracle SQL.

The migration targets Google Cloud Platform, specifically:
*   **Google BigQuery:** For all data processing logic, parameter handling, and internal orchestration via BigQuery SQL Stored Procedures.
*   **Cloud Composer (Apache Airflow):** For external job scheduling, parameter passing, and overall workflow management.

The goal is to leverage serverless, managed services for improved scalability, maintainability, and cost efficiency.

## 2. Generated artifacts

The following files were generated as part of this migration:

*   **`bigquery/dataset/d_ausd_bp_ta_apn_vertrag_proc.sql`**
    *   **Role:** BigQuery Stored Procedure. This procedure encapsulates the core data transformation logic originally found in `d_ausd_bp_ta_apn_vertrag.sql`. It processes APN contract data, aggregates information, and inserts it into the target table (`sof$ta_apn_vertrag`).
*   **`bigquery/dataset/k_ausd_bp_ta_apn_vertrag_sp.sql`**
    *   **Role:** BigQuery Stored Procedure. This is the main orchestration procedure, replacing the `k_ausd_bp_ta_apn_vertrag.ksh` shell script. It handles parameter validation, date derivation, calls `d_ausd_bp_ta_apn_vertrag_proc` for data processing, records job status in an audit log, and manages error handling.
*   **`bigquery/dataset/job_audit_table.sql`**
    *   **Role:** BigQuery DDL (Data Definition Language). This script defines the schema for the `job_audit` table, which will store execution details, parameters, status, and record counts for each run of the migrated job.
*   **`bigquery/dataset/error_log_table.sql`**
    *   **Role:** BigQuery DDL. This script defines the schema for the `error_log` table, used to capture detailed error messages and stack traces when the BigQuery stored procedures encounter issues.
*   **`airflow/dags/k_ausd_bp_ta_apn_vertrag_dag.py`**
    *   **Role:** Apache Airflow DAG (Directed Acyclic Graph). This Python script defines the workflow for the job in Cloud Composer. It is responsible for scheduling the job and triggering the `k_ausd_bp_ta_apn_vertrag_sp` BigQuery Stored Procedure, passing the necessary parameters.
*   **`config/k_ausd_bp_ta_apn_vertrag_config.yaml`**
    *   **Role:** Configuration file. This YAML file centralizes key parameters such as BigQuery project/dataset IDs, default job parameters, and orchestration settings, making the solution more configurable and environment-agnostic.

## 3. Key design decisions

The migration strategy involved several key design decisions to transform the KornShell-based workflow into a BigQuery-native, cloud-orchestrated solution:

*   **Orchestration Layer Shift:** The original shell script's role as an orchestrator was split.
    *   **Internal Orchestration:** Parameter parsing, validation, date derivation, and calling the core SQL logic are now handled within the `k_ausd_bp_ta_apn_vertrag_sp` BigQuery Stored Procedure, leveraging BigQuery SQL Scripting capabilities. This keeps the core logic and its immediate control flow within the data platform.
    *   **External Orchestration:** Job scheduling, triggering, and overall workflow management are moved to Cloud Composer (Airflow). This provides a robust, scalable, and observable orchestration platform, replacing the legacy shell-based scheduling.
*   **Parameter Handling:** The `ksh` `getopts` and custom shell functions for parameter validation (`pruefeParameterGesetzt`) are replaced by explicit parameters in the BigQuery Stored Procedures and BigQuery SQL `IF` statements with `RAISE` for validation. This integrates parameter handling directly into the BigQuery environment.
*   **Date Derivation:** The external `gestern.ksh` script is replaced by native BigQuery date functions (`CURRENT_DATE()`, `DATE_SUB`), simplifying dependencies and improving performance.
*   **SQL Execution:** The `starteSQLSkript` shell function, which invoked SQLPlus, is replaced by a direct `CALL` to the `d_ausd_bp_ta_apn_vertrag_proc` BigQuery Stored Procedure. This eliminates the need for external database clients and integrates seamlessly with BigQuery.
*   **Temporary Data & Record Counts:** File-based temporary outputs (e.g., `$DW_DIR_UTL/bert_k_ausd_bp_ta_apn_vertrag.tmp`) and subsequent `cat` operations for record counting are replaced by direct `COUNT(*)` queries on the target tables within BigQuery, or by using BigQuery temporary tables/variables if intermediate results are needed. This avoids file I/O and keeps data processing within BigQuery.
*   **Logging and Error Handling:** The custom `DWMSG_MeldeFehler` framework is replaced by BigQuery's `BEGIN...EXCEPTION` blocks, `RAISE` statements, and dedicated `job_audit` and `error_log` tables. This provides structured, queryable logging and error reporting within BigQuery.
*   **Commented Code:** Sections of commented-out shell commands (`sed`, `sort`, `join`) were assumed to remain inactive and were not migrated. This decision avoids unnecessary complexity for features not currently in use. If these features become active, they would require separate BigQuery SQL translation or potentially a different processing approach (e.g., Cloud Dataflow).

**Notable Trade-offs:**
*   **BigQuery-Specific Syntax:** The solution is highly dependent on BigQuery SQL Scripting, which requires specific BigQuery knowledge for maintenance and debugging.
*   **Initial Setup Overhead:** Requires setting up BigQuery datasets, tables, and a Cloud Composer environment, which is more involved than simply running a shell script.
*   **Loss of Direct File System Access:** Operations that relied on local file system manipulation (e.g., `sed`, `sort`) are either re-engineered into SQL or would require external services if reactivated.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:**
    *   Create the primary BigQuery dataset, e.g., `isbert_dataset` (as specified by `BIGQUERY_DATASET` in the Airflow DAG and `dataset_name` in the config).
    *   Create the BigQuery dataset for source-like tables, e.g., `isbert_schema` (as specified by `ISBERT_SCHEMA_DATASET` in the Airflow DAG and `isbert_schema_dataset` in the config).
2.  **BigQuery Table Creation:**
    *   Execute the DDL scripts:
        *   `bigquery/dataset/job_audit_table.sql` to create the `job_audit` table.
        *   `bigquery/dataset/error_log_table.sql` to create the `error_log` table.
    *   Ensure the source table `sof$ta_bpr_apn` and the target table `sof$ta_apn_vertrag` exist within the `isbert_dataset` (or the dataset specified by `p_dataset_name` in the stored procedures) with their appropriate schemas. These tables are external dependencies and their creation/loading is outside the scope of this migration.
3.  **BigQuery Stored Procedure Deployment:**
    *   Deploy `bigquery/dataset/d_ausd_bp_ta_apn_vertrag_proc.sql` to the `isbert_dataset`.
    *   Deploy `bigquery/dataset/k_ausd_bp_ta_apn_vertrag_sp.sql` to the `isbert_dataset`.
4.  **IAM Permissions Configuration:**
    *   The Google Cloud service account associated with your Cloud Composer environment (or the user executing the job) must have the following BigQuery roles for the `isbert_dataset` and `isbert_schema` datasets:
        *   `BigQuery Data Editor` (for inserting/updating `job_audit`, `error_log`, and `sof$ta_apn_vertrag`).
        *   `BigQuery Data Viewer` (for reading from `sof$ta_bpr_apn` and other potential source tables).
        *   `BigQuery Job User` (for running queries and stored procedures).
        *   `BigQuery Metadata Viewer` (for viewing table/procedure definitions).
5.  **Cloud Composer (Airflow) Deployment:**
    *   Update the `airflow/dags/k_ausd_bp_ta_apn_vertrag_dag.py` with your specific `BIGQUERY_PROJECT`, `BIGQUERY_DATASET`, and `ISBERT_SCHEMA_DATASET` values.
    *   Upload the `k_ausd_bp_ta_apn_vertrag_dag.py` file to your Cloud Composer environment's DAGs folder.
    *   Verify the DAG appears in the Airflow UI and is unpaused.
6.  **Configuration Review:**
    *   Review and update `config/k_ausd_bp_ta_apn_vertrag_config.yaml` with environment-specific values, especially `project_id`, `dataset_name`, `isbert_schema_dataset`, and `email_on_failure`.

## 5. Known gaps & unresolved references

The following items have been identified as known gaps or require further attention:

*   **Lineage Detail Ambiguity:** The automated lineage detection for the original script was incomplete. The migration design is based on static code analysis. There is a risk that dynamic dependencies or implicit behaviors not evident from static analysis might have been missed. Further manual analysis or runtime observation may be required.
*   **`r_ausd_bp_ta_apn_vertrag.ksh` Relationship:** The original script's purpose note mentions it as a "Kontrollscript zu r_ausd_bp_ta_apn_vertrag.ksh". The exact relationship and whether `r_ausd_bp_ta_apn_vertrag.ksh` is a dependency that also requires migration, or if `k_ausd_bp_ta_apn_vertrag.ksh` is the primary entry point, is unclear. This needs clarification.
*   **Environment Variable Mapping (`BERT_DIR_ROOT`, `DW_DIR_UTL`):** These legacy environment variables are replaced by BigQuery dataset/table references or explicit parameters. Ensure all references are correctly mapped to the new BigQuery environment structure.
*   **`d_ausd_bp_ta_apn_vertrag.sql` Content Validation:** The migration of `d_ausd_bp_ta_apn_vertrag.sql` to `d_ausd_bp_ta_apn_vertrag_proc` assumes direct functional equivalence. A thorough review of the original Oracle SQL against the BigQuery SQL is crucial to ensure all business logic, data types, and performance characteristics are preserved.
*   **Commented Code Activation:** The commented-out sections involving `sed`, `sort`, and `join` in the original `ksh` script were not migrated. If these functionalities are ever reactivated, they will require a separate design phase to translate them into BigQuery SQL (e.g., string functions, window functions, `ORDER BY`, `JOIN`) or to integrate with other GCP services like Cloud Dataflow for complex file processing.
*   **Error Code Mapping:** The original script used specific error codes (e.g., 192, 193). While the migrated BigQuery SP raises errors with descriptive messages, a formal mapping of these legacy error codes to a BigQuery-compatible logging mechanism or a custom error code system might be beneficial for consistency.
*   **`PoolBasisprodukt` Table Role:** The script refers to `v_TabName='PoolBasisprodukt'`. The exact role of this table (source, target, intermediate) and its schema definition need to be confirmed during the `d_ausd_bp_ta_apn_vertrag.sql` migration.
*   **Auto-incrementing IDs for Audit/Error Logs:** The `job_audit_id` and `error_id` fields in the audit and error log tables are currently placeholders. BigQuery does not have native auto-incrementing columns. A strategy for generating unique IDs (e.g., using `GENERATE_UUID()` in the `INSERT` statement, or a sequence table) needs to be implemented.
*   **Source/Target Table Schemas:** The schemas for `sof$ta_bpr_apn` (source) and `sof$ta_apn_vertrag` (target) are assumed to be compatible with the BigQuery SQL. Any differences in data types or column names between the original Oracle environment and BigQuery must be addressed.

## 6. Validation

Validation ensures the migrated job functions correctly and produces accurate results.

**How to run the tests:**

1.  **BigQuery Console Execution (Unit Test):**
    *   Manually execute the `d_ausd_bp_ta_apn_vertrag_proc` stored procedure in the BigQuery console with sample `p_dataset_name` and `p_isbert_schema_dataset` parameters.
    *   Manually execute the `k_ausd_bp_ta_apn_vertrag_sp` stored procedure in the BigQuery console, providing sample values for `p_JobKennung`, `p_EintragsNr`, `p_Stichtag` (e.g., `'01012023'`), `p_wiederanlaufWert`, `p_dataset_name`, and `p_isbert_schema_dataset`.
    *   Observe the output and check for errors.
2.  **Cloud Composer (Airflow) Trigger (Integration Test):**
    *   In the Airflow UI, unpause the `k_ausd_bp_ta_apn_vertrag_workflow` DAG.
    *   Manually trigger the DAG, providing test parameters for `job_kennung`, `eintrags_nr`, `stichtag`, and `wiederanlauf_wert` if needed.
    *   Monitor the DAG run in the Airflow UI for successful completion.
3.  **Data Validation:**
    *   Execute the migrated job against a dedicated test dataset in BigQuery.
    *   Run the original `ksh` script against a comparable test environment (if available) to generate a baseline output.
    *   Compare the data in the target table (`sof$ta_apn_vertrag`) in BigQuery with the output from the original system. Focus on record counts, key fields, and a representative sample of data.

**What "passing" means:**

*   **Execution Success:**
    *   All BigQuery Stored Procedures execute without raising unhandled exceptions.
    *   The Airflow DAG completes successfully (indicated by a "green" status in the Airflow UI).
*   **Audit & Logging:**
    *   The `job_audit` table contains a new entry for the job run with a `status` of 'SUCCESS'.
    *   The `record_count` in the `job_audit` table matches the expected number of records processed/inserted.
    *   There are no new entries in the `error_log` table related to this job run.
*   **Data Integrity:**
    *   The target table (`sof$ta_apn_vertrag`) in BigQuery is populated with data.
    *   The schema of the target table matches expectations.
    *   Record counts in the target table are consistent with the source system's output for the same input.
    *   A sample-based data comparison confirms that the transformed data in BigQuery is functionally equivalent to the data produced by the original system.

## 7. Rollback procedure

In case of issues during or after go-live, the following rollback procedure can be followed:

1.  **Immediate Action (Airflow):**
    *   In the Cloud Composer (Airflow) UI, pause or delete the `k_ausd_bp_ta_apn_vertrag_workflow` DAG to prevent further execution of the migrated job.
    *   Re-enable the original scheduling mechanism for the `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_apn_vertrag.ksh` script.
2.  **BigQuery Object Reversion:**
    *   **Stored Procedures:** Drop the newly deployed BigQuery Stored Procedures:
        ```sql
        DROP PROCEDURE IF EXISTS `dataset.k_ausd_bp_ta_apn_vertrag_sp`;
        DROP PROCEDURE IF EXISTS `dataset.d_ausd_bp_ta_apn_vertrag_proc`;
        ```
    *   **Target Data:** If the `sof$ta_apn_vertrag` table was modified or overwritten by the migrated job and the data is incorrect, restore it from the most recent valid backup or snapshot. If no backup exists, data recovery might be complex.
    *   **Audit/Error Tables:** If the `job_audit` and `error_log` tables were created solely for this migration and are not used by other processes, they can be dropped:
        ```sql
        DROP TABLE IF EXISTS `dataset.job_audit`;
        DROP TABLE IF EXISTS `dataset.error_log`;
        ```
3.  **Configuration Reversion:**
    *   Remove or archive the `config/k_ausd_bp_ta_apn_vertrag_config.yaml` file from the production environment.
4.  **Monitoring and Verification:**
    *   Verify that the original `ksh` job is running as expected and producing correct output.
    *   Confirm that no new BigQuery jobs related to the migration are being triggered.

**Important Considerations:**
*   **Data Backups:** Ensure a robust data backup and recovery strategy is in place for all BigQuery tables affected by the migration, especially the target table `sof$ta_apn_vertrag`, before initiating the go-live.
*   **Communication:** Inform all relevant stakeholders about the rollback and the status of the data processing.