# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the legacy ETL job identified by `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_geschaeftspartner.ksh`.

The original job consisted of:
*   A KornShell script (`k_ausd_geschaeftspartner.ksh`) responsible for orchestration, parameter validation, and execution of a core SQL script.
*   An Oracle SQL*Plus script (`d_ausd_geschaeftspartner.sql`) containing the primary ETL logic, including data extraction, transformation, and loading into various Oracle tables.

The job has been re-platformed to Google Cloud Platform (GCP), leveraging the following services:
*   **BigQuery:** As the target data warehouse for all staging, intermediate, and final transformed data, and for executing the core ETL logic via Stored Procedures.
*   **Cloud Composer (Apache Airflow):** For scheduling, orchestrating, and monitoring the migrated workflow.

The migration involved translating the KornShell orchestration logic into a BigQuery Stored Procedure and re-implementing the Oracle SQL*Plus ETL logic as another BigQuery Stored Procedure, ensuring functional equivalence and native BigQuery performance.

## 2. Generated Artifacts

The migration process generated the following artifacts:

*   **`your_project.your_dataset_procs.d_ausd_geschaeftspartner_proc.sql`**
    *   **Role:** This BigQuery Stored Procedure encapsulates the core ETL logic previously found in `d_ausd_geschaeftspartner.sql`. It is responsible for reading from BigQuery staging tables, truncating target tables, performing complex transformations (joins, data type conversions, function replacements), and inserting data into the final BigQuery target tables (`sof_ta_segm_prem`, `sof_ta_bpr_dn_evn_his`, `sof_ta_bpr_dn_evn`, `sof_ta_p_gesch_part`, `sof_ta_p_dn_nutzer`, `sof_ta_p_evn_empf`). It also returns the count of processed records.

*   **`your_project.your_dataset_procs.k_ausd_geschaeftspartner_main.sql`**
    *   **Role:** This BigQuery Stored Procedure replaces the orchestration logic of the original `k_ausd_geschaeftspartner.ksh` script. It handles input parameter validation, derives necessary date values, calls the `d_ausd_geschaeftspartner_proc` ETL procedure, and manages job logging to a dedicated BigQuery log table. It serves as the main entry point for the Airflow DAG.

*   **`your_project.your_dataset_logging.job_log_create_table.sql`**
    *   **Role:** This DDL script defines the `job_log` table in BigQuery. This table is used by `k_ausd_geschaeftspartner_main` to record job start/end times, status, messages, and any errors, providing a centralized logging mechanism for the migrated job.

*   **`dags/k_ausd_geschaeftspartner_dag.py`**
    *   **Role:** This Python script defines an Apache Airflow DAG. It is responsible for scheduling and executing the `k_ausd_geschaeftspartner_main` BigQuery Stored Procedure. It configures the parameters passed to the stored procedure, including dynamic date generation for `p_Stichtag`.

## 3. Key Design Decisions

The migration approach was guided by the following key design decisions:

*   **BigQuery Native ETL:** The core ETL logic was re-implemented directly in BigQuery SQL as Stored Procedures. This leverages BigQuery's serverless, scalable, and cost-effective query engine, eliminating the need for external compute resources for transformations.
    *   **Trade-off:** Required significant manual translation of Oracle-specific SQL constructs (e.g., `NVL`, `DECODE`, `TO_CHAR`, `||`, `(+)` joins, `TRUNCATE` via PL/SQL package) to BigQuery SQL equivalents (`IFNULL`/`COALESCE`, `CASE`, `FORMAT_DATE`/`PARSE_DATE`, `CONCAT`, explicit `LEFT JOIN`, direct `TRUNCATE TABLE`). This ensures optimal performance within BigQuery but demanded careful re-engineering.
*   **BigQuery Stored Procedures for Orchestration:** The KornShell orchestration logic was also translated into a BigQuery Stored Procedure. This centralizes the entire workflow within BigQuery, allowing for atomic execution, consistent error handling, and simplified interaction with the data warehouse.
    *   **Trade-off:** Replaced shell-scripting features (parameter parsing, file operations, external script calls) with BigQuery SQL scripting features (`DECLARE`, `SET`, `ASSERT`, `CALL`, `INSERT` for logging). This reduces the complexity of managing multiple technologies but requires familiarity with BigQuery's procedural language.
*   **Cloud Composer (Airflow) for Scheduling:** Airflow was chosen for scheduling due to its robust capabilities for workflow orchestration, dependency management, monitoring, and integration with GCP services.
    *   **Trade-off:** Introduces a new technology stack (Python, Airflow concepts) compared to the simple cron-based scheduling of the original ksh script. However, it provides greater visibility, retry mechanisms, and scalability for future ETL needs.
*   **Dedicated BigQuery Logging Table:** Instead of disparate log files or `echo` statements, a structured `job_log` table in BigQuery was introduced. This centralizes logging, making it easier to monitor job execution, debug failures, and analyze performance.
    *   **Trade-off:** Requires explicit `INSERT` statements for logging within the Stored Procedures, adding a small overhead compared to simple `echo` commands.
*   **Managed Data Ingestion for External Sources:** The design prioritizes establishing robust ingestion pipelines (e.g., Datastream, Dataflow) to bring external Oracle source data into BigQuery staging tables. This decouples the ETL job from direct, real-time dependencies on external Oracle databases via database links.
    *   **Trade-off:** Requires upfront investment in building and maintaining ingestion pipelines. However, it significantly improves reliability, performance, and reduces operational overhead compared to federated queries or direct database link usage within BigQuery ETL.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be completed:

1.  **GCP Project Setup:**
    *   Ensure a GCP project (`your_project`) is provisioned and configured.

2.  **BigQuery Dataset Creation:**
    *   Create the following BigQuery datasets in your GCP project:
        *   `your_project.your_dataset_procs`: To host the BigQuery Stored Procedures.
        *   `your_project.your_dataset_staging`: To host the ingested source data from Oracle.
        *   `your_project.your_dataset_target`: To host the final target tables and intermediate tables.
        *   `your_project.your_dataset_logging`: To host the `job_log` table.

3.  **IAM Permissions:**
    *   Ensure the service account used by Cloud Composer (Airflow) has the following BigQuery roles:
        *   `BigQuery Data Editor` on `your_project.your_dataset_staging`, `your_project.your_dataset_target`, and `your_project.your_dataset_logging` (for reading staging, writing to target/logging).
        *   `BigQuery Job User` on `your_project` (for running BigQuery jobs).
        *   `BigQuery Data Viewer` on `your_project.your_dataset_procs` (for executing stored procedures).
    *   Ensure the service account has appropriate permissions for Cloud Composer (e.g., `Composer Worker` role).

4.  **Data Ingestion Pipeline Setup:**
    *   **Crucial Pre-requisite:** Implement and verify the data ingestion pipeline(s) to bring the following Oracle source tables into `your_project.your_dataset_staging`:
        *   `bpd$ta_bp_valueseg_assoc` (as `bpd_ta_bp_valueseg_assoc`)
        *   `pds$ta_bpri_com` (as `pds_ta_bpri_com`)
        *   `isbert_schema.dwtk_meldungen` (as `dwtk_meldungen`)
        *   `sof$ta_e_reach_gp` (as `sof_ta_e_reach_gp`)
        *   `sof$ta_e_business_gp` (as `sof_ta_e_business_gp`)
        *   `sof$ta_e_reach_dn` (as `sof_ta_e_reach_dn`)
        *   `sof$ta_e_business_dn` (as `sof_ta_e_business_dn`)
        *   `sof$ta_e_reach_ev` (as `sof_ta_e_reach_ev`)
        *   `sof$ta_e_business_ev` (as `sof_ta_e_business_ev`)
    *   These tables must exist and be populated in the staging dataset before the ETL job can run successfully.

5.  **Deploy BigQuery Stored Procedures:**
    *   Execute the DDL for `your_project.your_dataset_procs.d_ausd_geschaeftspartner_proc.sql` and `your_project.your_dataset_procs.k_ausd_geschaeftspartner_main.sql` in BigQuery.
    *   Execute the DDL for `your_project.your_dataset_logging.job_log_create_table.sql` in BigQuery.

6.  **Deploy Airflow DAG:**
    *   Upload `dags/k_ausd_geschaeftspartner_dag.py` to the DAGs folder of your Cloud Composer environment.
    *   **Update Placeholders:** Edit the DAG file to replace `your_project` and `your_dataset_procs` with your actual GCP project ID and dataset names.
    *   **Configure Schedule:** Set the `schedule` parameter in the DAG to your desired execution frequency (e.g., `schedule="@daily"`).

7.  **Airflow Connection:**
    *   Ensure the `google_cloud_default` connection is configured correctly in your Airflow environment.

## 5. Known Gaps & Unresolved References

The following items are noted as known gaps, unresolved references, or areas for potential follow-up:

*   **Placeholder Values:** The generated code uses placeholders like `your_project`, `your_dataset_procs`, `your_dataset_staging`, `your_dataset_target`, and `your_dataset_logging`. These *must* be replaced with actual GCP project and BigQuery dataset IDs before deployment.
*   **Oracle SQL*Plus Specific Syntax:** SQL*Plus commands (`prompt`, `spool`, `WHENEVER SQLERROR`, `COLUMN`, `start`) have been removed as they have no direct BigQuery equivalents. Their original purpose (e.g., logging, error control) has been addressed through BigQuery scripting and the `job_log` table, but the exact fidelity of their behavior might differ.
*   **Commented-Out Functionality:** The original KornShell script contained commented-out references to `FOSJobDeaktivate` and `FOSJobErzeugeEintrag`. These functionalities have not been migrated. A decision is required on whether these job management features are still needed. If so, they would require a separate design and implementation, likely involving BigQuery control tables and procedures.
*   **Performance Optimization (B4 Item):** While BigQuery automatically parallelizes queries, the translated SQL queries should be reviewed for BigQuery-specific performance best practices (e.g., partitioning, clustering, efficient join strategies, avoiding anti-patterns). This is an ongoing optimization task post-migration.
*   **Error Handling Fidelity (B4 Item):** The `WHENEVER SQLERROR CONTINUE/EXIT FAILURE` logic from Oracle has been mapped to BigQuery's `EXCEPTION WHEN ERROR` block and `ASSERT` statements. While this provides robust error handling, a thorough review of specific error scenarios and their reporting mechanisms is recommended to ensure full fidelity with the legacy system's error reporting.
*   **`p_Stichtag` Parameter in Airflow:** The DAG dynamically generates `p_Stichtag` using `ds_nodash` and converts it to `DDMMYYYY`. This assumes a daily run and that `ds_nodash` (YYYYMMDD) is the correct base. Verify this logic aligns with the intended `Stichtag` derivation.
*   **External Data Ingestion Robustness:** The success of this migrated job heavily relies on the upstream data ingestion pipelines (Phase 1 of the build plan) being robust, reliable, and delivering data to the BigQuery staging tables with the expected schema and timeliness. Any issues in ingestion will directly impact this job.

## 6. Validation

Validation ensures that the migrated job functions correctly and produces equivalent results to the legacy system.

**How to Run Tests:**

1.  **Unit Test BigQuery Stored Procedures:**
    *   Execute `d_ausd_geschaeftspartner_proc` directly in BigQuery with sample data in the staging tables and specific input parameters.
    *   Execute `k_ausd_geschaeftspartner_main` directly in BigQuery with sample parameters.
    *   Monitor the `job_log` table for entries.

2.  **Run Airflow DAG:**
    *   Trigger the `k_ausd_geschaeftspartner_dag` manually from the Cloud Composer UI.
    *   Observe the task logs in Airflow for successful execution.
    *   Check the BigQuery `job_log` table for entries corresponding to the DAG run.

3.  **Data Validation:**
    *   **Pre-requisite:** Ensure the BigQuery staging tables are populated with data that is functionally equivalent to the Oracle source data used by the legacy job for a specific test date (`Stichtag`).
    *   **Comparison:** After a successful run of the migrated job, compare the data in the BigQuery target tables (`sof_ta_segm_prem`, `sof_ta_bpr_dn_evn_his`, `sof_ta_bpr_dn_evn`, `sof_ta_p_gesch_part`, `sof_ta_p_dn_nutzer`, `sof_ta_p_evn_empf`) with the corresponding output from the legacy Oracle job for the same `Stichtag`. This can be done using SQL queries to count rows, checksums, or detailed row-by-row comparisons.

**What "Passing" Means:**

*   **Airflow DAG:** The `k_ausd_geschaeftspartner_dag` completes successfully without any failed tasks.
*   **BigQuery Stored Procedures:** Both `k_ausd_geschaeftspartner_main` and `d_ausd_geschaeftspartner_proc` execute without raising unhandled exceptions.
*   **Logging:** The `your_project.your_dataset_logging.job_log` table contains `INFO` level entries indicating job start and successful completion, with no `ERROR` level entries for the specific run.
*   **Record Counts:** The `records_processed` value logged in `job_log` (returned by `d_ausd_geschaeftspartner_proc`) matches the record count reported by the legacy Oracle job for the same `Stichtag`.
*   **Data Equivalence:** The data in the BigQuery target tables is identical (or functionally equivalent, considering data type differences) to the data produced by the legacy Oracle job for the same input parameters and source data. This includes:
    *   Matching row counts in all target tables.
    *   Matching values for key columns and aggregated metrics.
    *   No unexpected data truncation or transformation errors.

## 7. Rollback Procedure

In case of critical issues or failure during go-live, the following rollback procedure can be executed:

1.  **Stop New Workflow:**
    *   Pause or disable the `k_ausd_geschaeftspartner_dag` in Cloud Composer to prevent further execution of the migrated job.

2.  **Revert to Legacy Job:**
    *   Re-enable and restart the original legacy KornShell job (`k_ausd_geschaeftspartner.ksh`) on the legacy platform. Ensure it has access to its original Oracle source and target tables.

3.  **Data Cleanup (Optional but Recommended):**
    *   If the migrated job has written data to the BigQuery target tables (`your_project.your_dataset_target.*`), consider truncating or deleting the data from these tables for the affected period to avoid data inconsistencies if the legacy job is now writing to its original targets.
    *   `TRUNCATE TABLE your_project.your_dataset_target.sof_ta_segm_prem;`
    *   `TRUNCATE TABLE your_project.your_dataset_target.sof_ta_bpr_dn_evn;`
    *   `TRUNCATE TABLE your_project.your_dataset_target.sof_ta_bpr_dn_evn_his;`
    *   `TRUNCATE TABLE your_project.your_dataset_target.sof_ta_p_gesch_part;`
    *   `TRUNCATE TABLE your_project.your_dataset_target.sof_ta_p_dn_nutzer;`
    *   `TRUNCATE TABLE your_project.your_dataset_target.sof_ta_p_evn_empf;`
    *   (Note: The `job_log` table can typically be retained for auditing purposes.)

4.  **Cleanup Migrated Artifacts (Optional):**
    *   If the rollback is deemed permanent or for a significant period, you may choose to delete the BigQuery Stored Procedures and the Airflow DAG from the GCP environment.
        *   `DROP PROCEDURE your_project.your_dataset_procs.k_ausd_geschaeftspartner_main;`
        *   `DROP PROCEDURE your_project.your_dataset_procs.d_ausd_geschaeftspartner_proc;`
        *   Delete `dags/k_ausd_geschaeftspartner_dag.py` from the Cloud Composer DAGs folder.

This procedure ensures a quick return to the known working state of the legacy system while providing options for cleaning up the partially deployed new system.