# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the ETL job `BERT_AUSTAUSCH_KSH`, originally identified as `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_austausch.ksh`. The job's primary function is to prepare and provide a snapshot extract of the contract cache base table (`Stichtags-Abzug der Vertrags-Cache`) for the BERT report and for Forderungsscoring.

The migration involved re-engineering the existing KornShell scripts (`r_ausd_austausch.ksh`, `k_ausd_austausch.ksh`) and Oracle SQL (`d_ausd_austausch.sql`) into a BigQuery-native solution. The target platform is Google Cloud Platform, utilizing BigQuery for data storage and transformation, and Google Cloud Composer (Apache Airflow) for orchestration.

## 2. Generated artifacts

The migration process generated the following artifacts:

*   **`sql/ddl/bert_ddl.sql`**:
    *   **Role**: This SQL script defines the Data Definition Language (DDL) for all necessary tables in BigQuery. It includes:
        *   Creation of the target BigQuery dataset (`my_project.my_dataset`).
        *   DDL for all source tables (`sof_ta_p_rech_empf`, `sof_ta_p_vertrag`, `sof_ta_p_basisprod`, `sof_ta_p_gesch_part`, `sof_ta_p_dn_nutzer`, `sof_ta_p_evn_empf`, `sof_ta_p_discount`, `sof_ta_p_discount_rr`, `sof_ta_p_d1_vpn`) which are expected to be populated by an upstream ingestion process.
        *   DDL for the final target tables (`rpt_ta_s_d1_rech_empf`, `rpt_ta_s_d1_vertrag`, `rpt_ta_s_d1_rech_kunde`, `rpt_ta_s_d1_discount`, `rpt_ta_s_d1_discount_rr`, `rpt_ta_s_d1_vpn`).
        *   DDL for audit and metadata tables (`job_audit_log`, `job_sequence`) used for logging and unique ID generation.
        *   Initialization of the `job_sequence` table.

*   **`sql/stored_procedures/k_ausd_austausch.sql`**:
    *   **Role**: This BigQuery Stored Procedure encapsulates the core data transformation logic previously found in `k_ausd_austausch.ksh` and `d_ausd_austausch.sql`. It performs the following:
        *   Receives job parameters (`p_job_kennung`, `p_eintrags_nr`, `p_stichtag`, `p_wiederanlauf_wert`).
        *   Logs the start and end of the transformation, as well as any errors, to `job_audit_log`.
        *   Executes the series of `CREATE OR REPLACE TABLE AS SELECT` statements to populate the target `rpt_ta_s_d1_*` tables, applying all necessary joins, `CASE` statements, and function conversions from the original Oracle SQL.
        *   Manages intermediate data steps using Common Table Expressions (CTEs) where temporary tables were previously used in Oracle.

*   **`sql/stored_procedures/bert_austausch_ksh.sql` (Expected)**:
    *   **Role**: This BigQuery Stored Procedure (not fully provided in the snippet, but implied by the design) will act as the main wrapper. It will:
        *   Handle parameter parsing, date defaulting, and restart logic, mirroring `r_ausd_austausch.ksh`.
        *   Generate unique job IDs (if required) using the `job_sequence` table.
        *   Orchestrate the call to the `k_ausd_austausch` stored procedure.
        *   Manage overall job logging to `job_audit_log`.

*   **`dags/bert_austausch_ksh_dag.py` (Expected)**:
    *   **Role**: This Python script defines the Apache Airflow DAG. It will:
        *   Replace the UC4 scheduler for this job.
        *   Define the job's schedule.
        *   Use the `BigQueryOperator` or `BigQueryExecuteQueryOperator` to invoke the `bert_austausch_ksh` stored procedure in BigQuery, passing required parameters.
        *   Implement Airflow-native error handling, retries, and alerting mechanisms.

## 3. Key design decisions

*   **Cloud-Native Platform Adoption**: Migrating from on-premise Oracle and KornShell to Google Cloud's BigQuery and Cloud Composer (Airflow) leverages managed services for scalability, performance, and reduced operational overhead.
*   **BigQuery as Central Data Warehouse**: BigQuery is chosen for its analytical capabilities, serverless architecture, and cost-effectiveness for large-scale data processing, replacing Oracle as the primary data store for this ETL job.
*   **Stored Procedures for ETL Logic**: Translating complex Oracle SQL and shell script logic into BigQuery Stored Procedures centralizes the transformation logic within the data warehouse, improving maintainability and performance by reducing data movement.
*   **Airflow for Orchestration**: Cloud Composer (Airflow) provides robust scheduling, monitoring, and dependency management, replacing the legacy UC4 scheduler and offering greater flexibility and visibility into pipeline execution.
*   **Atomic Table Updates**: The `CREATE OR REPLACE TABLE AS SELECT` pattern is used for populating target tables. This ensures atomicity and avoids downtime during updates, effectively replacing Oracle's `TRUNCATE`/`INSERT` and `RENAME` operations.
*   **BigQuery-Native DDL Handling**: Oracle-specific DDL operations like `CREATE INDEX`, `ALTER INDEX`, `ANALYZE TABLE` are not directly translated. BigQuery's columnar storage, partitioning, clustering, and automatic query optimizer inherently handle performance aspects, eliminating the need for explicit index management.
*   **Consolidated Logging**: All job execution metadata, status, and error messages are directed to a structured `job_audit_log` table in BigQuery, replacing disparate file-based logging and enabling centralized monitoring and analysis.
*   **Function and Syntax Translation**: Oracle-specific SQL functions (e.g., `NVL`, `DECODE`, `(+)` for outer joins) are systematically converted to their BigQuery SQL equivalents (e.g., `IFNULL`, `CASE`, `LEFT JOIN`), ensuring functional parity.
*   **Intermediate Table Management**: Temporary Oracle tables (`sof$ta_rechdef`, `sof$ta_kd_kto`) are re-engineered as Common Table Expressions (CTEs) within the BigQuery stored procedures, optimizing query execution and reducing the need for persistent intermediate storage.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps are required:

1.  **BigQuery Dataset Creation**:
    *   Manually create the BigQuery dataset, e.g., `my_project.my_dataset`, in your GCP project. The `bert_ddl.sql` script includes `CREATE SCHEMA IF NOT EXISTS`, but it's good practice to ensure the project and dataset exist and are correctly named.
    *   **Action**: `bq mk --dataset --default_location=EU my_project:my_dataset` (adjust location as needed).

2.  **IAM & Permissions Configuration**:
    *   Ensure the service account used by Cloud Composer (Airflow) has the necessary BigQuery roles (e.g., `BigQuery Data Editor`, `BigQuery Job User`) to create/replace tables, execute stored procedures, and insert into audit logs.
    *   Ensure the service account used for data ingestion from Oracle has appropriate permissions to write to the BigQuery source tables.
    *   **Action**: Grant IAM roles via GCP Console or `gcloud` commands.

3.  **Oracle Data Ingestion Pipeline Setup**:
    *   Establish and configure the data ingestion pipeline responsible for extracting data from the Oracle source tables (`sof$ta_p_*`) and loading it into their corresponding BigQuery tables (`my_project.my_dataset.sof_ta_p_*`). This could involve Cloud Data Fusion, Dataflow, or a custom solution.
    *   **Action**: Deploy and configure the chosen ingestion mechanism.

4.  **Connection Strings & Secrets Management**:
    *   Securely store any credentials or connection details required for the Oracle data ingestion pipeline (e.g., Oracle database username, password, connection string) in a secrets management service like Google Secret Manager.
    *   **Action**: Store secrets in Secret Manager and configure the ingestion pipeline to retrieve them.

5.  **Initial Data Load**:
    *   Perform an initial full load of all Oracle source tables into their respective BigQuery `sof_ta_p_*` tables to establish the baseline data.
    *   **Action**: Execute the configured data ingestion pipeline for a full load.

6.  **Deploy BigQuery DDL and Stored Procedures**:
    *   Execute the `sql/ddl/bert_ddl.sql` script to create all necessary tables and initialize the `job_sequence` table.
    *   Deploy the `sql/stored_procedures/k_ausd_austausch.sql` and `sql/stored_procedures/bert_austausch_ksh.sql` (once generated) to BigQuery.
    *   **Action**: Use `bq query --use_legacy_sql=false <file_path>` or BigQuery UI to run the DDL and SP creation scripts.

7.  **Deploy Cloud Composer (Airflow) DAG**:
    *   Upload the `dags/bert_austausch_ksh_dag.py` file to the DAGs folder of your Cloud Composer environment.
    *   **Action**: Copy DAG file to GCS bucket associated with Composer.

8.  **Replace Placeholders**:
    *   Ensure all `my_project.my_dataset` placeholders in the generated SQL and DAG files are replaced with the actual GCP project ID and BigQuery dataset name.
    *   **Action**: Find and replace placeholders in all generated files.

## 5. Known gaps & unresolved references

*   **Oracle PL/SQL `runstatement` Calls**: The exact logic within `isbert_schema.dwpa_util_skript.runstatement` calls in the original Oracle script needs further investigation. These calls typically execute dynamic DDL or other utility functions. The current BigQuery stored procedures use `CREATE OR REPLACE TABLE AS SELECT` for atomic updates, which generally replaces the need for explicit `TRUNCATE`, `RENAME`, and index management. However, if `runstatement` performs other critical, non-standard operations, these might need specific BigQuery equivalents or alternative handling. This is flagged as a **B4 item** for detailed analysis.
*   **Performance Tuning for BigQuery**: While BigQuery is highly performant, complex queries with numerous joins and `CASE` statements (as seen in `rpt_ta_s_d1_vertrag`) might benefit from BigQuery-specific optimizations. This includes:
    *   Implementing appropriate **partitioning** (e.g., by date columns like `vertragsbeginn` or `geplant_kuend`) on large target tables.
    *   Defining **clustering keys** (e.g., `vertrag_id_carmen`, `rechdef_id_carmen`) on frequently joined or filtered columns.
    *   Consideration of **materialized views** for frequently accessed complex subqueries if performance becomes an issue.
    *   **Action**: Monitor query performance post-migration and apply BigQuery-specific tuning as needed.
*   **Data Volume and Ingestion Strategy**: The design assumes efficient data ingestion from Oracle to BigQuery. For very large source tables, the chosen ingestion method (e.g., Dataflow, Data Transfer Service, custom scripts) needs to be robustly designed and scaled to handle the volume and frequency of updates.
*   **Historical Data Preservation**: The "Stichtags-Abzug" (snapshot extract) implies a need for historical data. The current `CREATE OR REPLACE TABLE AS SELECT` strategy overwrites the target tables. If historical snapshots need to be maintained within BigQuery, a different strategy (e.g., appending to a partitioned table with a snapshot date, or using BigQuery's time-travel capabilities for short-term history) must be implemented.
*   **Dynamic SQL Confirmation**: The design assumes `d_ausd_austausch.sql` is a static script. If `k_ausd_austausch.ksh` dynamically alters `d_ausd_austausch.sql` based on runtime parameters beyond simple variable substitution, this would introduce significant complexity and require a more dynamic BigQuery stored procedure or Airflow task. Based on current analysis, this is not expected, but a final confirmation is recommended.

## 6. Validation

Validation should confirm both data integrity and functional correctness of the migrated job.

1.  **Data Ingestion Validation**:
    *   **Method**: After the initial and subsequent incremental loads from Oracle to BigQuery source tables (`sof_ta_p_*`), compare row counts and checksums (e.g., sum of numeric columns, hash of string columns) between the Oracle source tables and their BigQuery counterparts.
    *   **Passing Criteria**: Row counts must match exactly, and checksums/aggregates for key columns should be identical.

2.  **Functional Validation (Core Transformation)**:
    *   **Method**:
        *   Run the migrated Airflow DAG for a specific `stichtag` (processing date).
        *   Extract data from the final target tables in BigQuery (`rpt_ta_s_d1_*`) for the same `stichtag`.
        *   Run the original Oracle job for the *same* `stichtag` and extract data from its output tables.
        *   Perform a detailed row-by-row comparison of the output data between BigQuery and Oracle for a representative sample. Focus on key business fields and complex `CASE` logic outputs.
        *   Compare overall row counts and key aggregate metrics (SUM, AVG, COUNT DISTINCT) for each target table.
    *   **Passing Criteria**:
        *   Row counts for each `rpt_ta_s_d1_*` table must match between BigQuery and Oracle.
        *   Key aggregate metrics (e.g., sum of `RABATT_ALLE`, count of `VERTRAG_ID_CARMEN`) must match.
        *   A sample of individual records should show identical values for all columns, especially those derived from complex transformations.

3.  **Logging and Monitoring Validation**:
    *   **Method**: Trigger the Airflow DAG and monitor its execution in the Cloud Composer UI. After completion, query the `my_project.my_dataset.job_audit_log` table.
    *   **Passing Criteria**:
        *   The Airflow DAG run should complete successfully without errors.
        *   The `job_audit_log` table should contain entries for the job's start, successful completion, and any intermediate informational messages, with correct `stichtag` and `restart_value`. No `ERROR` entries should be present for a successful run.

## 7. Rollback procedure

In case of critical issues or failure during go-live, the following rollback procedure can be executed to revert to the original Oracle/UC4 system:

1.  **Deactivate New Airflow DAG**:
    *   Pause or delete the `bert_austausch_ksh_dag.py` in the Cloud Composer UI to prevent further execution of the migrated job.
    *   **Action**: Navigate to Cloud Composer -> DAGs, find `bert_austausch_ksh_dag`, and toggle it off or delete it.

2.  **Re-enable Original UC4 Job**:
    *   Reactivate the original `BERT_AUSTAUSCH_KSH` job in the UC4 scheduler.
    *   **Action**: Access UC4 and re-enable the job.

3.  **Revert BigQuery Target Tables (Optional but Recommended)**:
    *   If the new BigQuery target tables (`rpt_ta_s_d1_*`) were exposed to downstream systems or if their state needs to be reset, revert them.
    *   **Option A (Time Travel)**: If BigQuery's time travel feature is sufficient (up to 7 days), you can restore tables to a point before the problematic run.
        *   **Action**: `CREATE OR REPLACE TABLE my_project.my_dataset.rpt_ta_s_d1_rech_empf AS SELECT * FROM my_project.my_dataset.rpt_ta_s_d1_rech_empf FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL X MINUTE);` (repeat for all target tables, adjusting `X` as needed).
    *   **Option B (Backup Restore)**: If backups of the BigQuery target tables were taken before go-live, restore from those backups.
        *   **Action**: Restore tables from backup snapshots or copies.
    *   **Option C (Clear Tables)**: If simply clearing the tables is acceptable, truncate them.
        *   **Action**: `TRUNCATE TABLE my_project.my_dataset.rpt_ta_s_d1_rech_empf;` (repeat for all target tables).

4.  **Stop/Revert BigQuery Ingestion (if necessary)**:
    *   If the Oracle-to-BigQuery data ingestion pipeline is causing issues or interfering with the rollback, pause or stop it.
    *   **Action**: Pause or stop the relevant Dataflow job, Data Transfer Service, or custom ingestion script.

5.  **Monitor Original System**:
    *   Verify that the original Oracle/UC4 job is running correctly and producing expected outputs.
    *   **Action**: Monitor UC4 logs and Oracle database for successful job execution.