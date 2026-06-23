# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the `DW.BERT_AUSD_V_TA_P_DISCOUNT` job. This ETL workflow, originally orchestrated by UC4 and executing KornShell and Oracle SQL scripts, is designed to enrich discount information with contract numbers and templates.

The job has been migrated from its legacy environment (UC4 scheduler, KornShell scripts, Oracle SQL*Plus against an Oracle database) to Google Cloud Platform (GCP). The target platform leverages:
*   **BigQuery** for data warehousing, transformations, and centralized logging/auditing.
*   **Cloud Composer (Airflow)** for workflow orchestration.

The core logic, which involves truncating and reloading discount tables with contract details, has been refactored into BigQuery Stored Procedures. The shell script orchestration and parameter handling have also been translated into BigQuery Stored Procedures, which are then invoked by Airflow DAGs.

## 2. Generated artifacts

The migration process generated the following files:

*   **`sql/stored_procedures/sp_k_ausd_v_ta_p_discount_rr.sql`**
    *   **Role:** BigQuery Stored Procedure. This procedure is the direct migration of the legacy `k_ausd_v_ta_p_discount_rr.ksh` KornShell script. It orchestrates the execution of the SQL transformation for `ta_p_discount_rr` by calling `sp_d_ausd_v_ta_p_discount_rr` and handles basic logging and error propagation.
*   **`sql/stored_procedures/sp_k_ausd_v_ta_p_discount.sql`**
    *   **Role:** BigQuery Stored Procedure. This procedure is the direct migration of the legacy `k_ausd_v_ta_p_discount.ksh` KornShell script. It orchestrates the execution of the SQL transformation for `ta_p_discount` by calling `sp_d_ausd_v_ta_p_discount` and handles basic logging and error propagation.
*   **`sql/stored_procedures/sp_r_ausd_v_ta_p_discount_rr.sql`**
    *   **Role:** BigQuery Stored Procedure. This procedure is the direct migration of the legacy `r_ausd_v_ta_p_discount_rr.ksh` KornShell script. It acts as the top-level wrapper, handling job initialization, generating a unique `job_id`, managing job control status (RUNNING, COMPLETED, FAILED), and calling `sp_k_ausd_v_ta_p_discount_rr`. It accepts a `p_processing_date` parameter.
*   **`sql/stored_procedures/sp_r_ausd_v_ta_p_discount.sql`**
    *   **Role:** BigQuery Stored Procedure. This procedure is the direct migration of the legacy `r_ausd_v_ta_p_discount.ksh` KornShell script. It acts as the top-level wrapper, handling job initialization, generating a unique `job_id`, managing job control status (RUNNING, COMPLETED, FAILED), and calling `sp_k_ausd_v_ta_p_discount`. It accepts a `p_processing_date` parameter.
*   **`dags/dw_bert_ausd_v_ta_p_discount_rr.py`**
    *   **Role:** Airflow DAG. This DAG is the migration of the legacy `DW.BERT_AUSD_V_TA_P_DISCOUNT_RR.xml` UC4 job definition. It contains a single `BigQueryExecuteStoredProcedureOperator` task that invokes the `sp_r_ausd_v_ta_p_discount_rr` BigQuery Stored Procedure, passing the Airflow execution date (`ds`) as the `p_processing_date` parameter.
*   **`dags/dw_bert_ausd_v_ta_p_discount.py`**
    *   **Role:** Airflow DAG. This DAG is the migration of the legacy `DW.BERT_AUSD_V_TA_P_DISCOUNT.xml` UC4 job definition. It contains a single `BigQueryExecuteStoredProcedureOperator` task that invokes the `sp_r_ausd_v_ta_p_discount` BigQuery Stored Procedure, passing the Airflow execution date (`ds`) as the `p_processing_date` parameter.

## 3. Key design decisions

*   **UC4 to Cloud Composer (Airflow) for Orchestration:** Airflow provides robust, cloud-native workflow management, replacing the legacy UC4 scheduler. Each UC4 job definition is translated into a dedicated Airflow DAG for clear separation and management.
*   **KornShell to BigQuery Stored Procedures:** The KornShell scripts primarily acted as orchestrators for SQL scripts, handling parameters, environment setup, and basic logging. Migrating these to BigQuery Stored Procedures (`sp_r_*` and `sp_k_*`) allows the entire workflow logic (orchestration and transformation) to reside within BigQuery, leveraging its performance and scalability. This avoids the overhead of external compute (like Dataproc) for purely SQL-based logic.
*   **Oracle SQL to BigQuery SQL within Stored Procedures:** The core data transformation logic from the Oracle SQL scripts (`d_ausd_v_ta_p_discount_rr.sql`, `d_ausd_v_ta_p_discount.sql`) is directly translated into BigQuery SQL. This includes `TRUNCATE TABLE` and `INSERT INTO ... SELECT` statements, with Oracle-specific syntax (e.g., `NVL`, `TO_CHAR`, `/*+ parallel */` hints) adapted to BigQuery SQL equivalents (e.g., `IFNULL`, `FORMAT_TIMESTAMP`).
*   **Centralized Logging and Auditing in BigQuery:** The legacy KornShell scripts had custom logging mechanisms. In the new design, dedicated BigQuery tables (`job_control`, `job_log`, `job_error_log`) are used to capture job execution status, logs, and errors. This provides a unified, queryable source for monitoring and troubleshooting.
*   **Parameter Handling via Stored Procedure Arguments:** Legacy `getopts`-style parameter parsing in KornShell is replaced by explicit parameters in BigQuery Stored Procedures. Airflow DAGs pass relevant execution context (like `ds` for `p_processing_date`) as arguments to the top-level stored procedures.
*   **Direct BigQuery Stored Procedure Execution from Airflow:** Instead of using `DataprocSubmitJobOperator` to run PySpark scripts (which was an initial consideration for UC4 migrations), `BigQueryExecuteStoredProcedureOperator` is chosen. This is more efficient and direct for SQL-heavy workloads, as it avoids spinning up and managing Dataproc clusters.
*   **Handling of Oracle DB-Link (`@pcrs1`):** The external Oracle "Carmen" database, identified as a critical source, requires a separate data ingestion strategy. The design assumes these source tables (`dwtk_meldungen`, `ta_discount_rr`, `ta_cntrct_crs`, `ta_cntrct_templ`, `ta_disc_zusgf`) will be pre-migrated or continuously synchronized into BigQuery. This is a prerequisite for the migrated job to function.

## 4. Manual steps before go-live

Before the migrated job can be run in production, the following manual steps must be completed:

1.  **GCP Project and BigQuery Dataset Setup:**
    *   Ensure a GCP project (`your-gcp-project`) is set up and billing is enabled.
    *   Create the target BigQuery dataset (`your_dataset`) where all tables and stored procedures will reside.
        ```bash
        bq mk --project_id your-gcp-project your_dataset
        ```

2.  **BigQuery Table Creation (Source Data):**
    *   Create the necessary source tables in BigQuery, mirroring the schema of the Oracle source tables. These tables will be populated by the data ingestion pipeline (see step 3).
        *   `your-gcp-project.your_dataset.dwtk_meldungen`
        *   `your-gcp-project.your_dataset.ta_discount_rr`
        *   `your-gcp-project.your_dataset.ta_cntrct_crs`
        *   `your-gcp-project.your_dataset.ta_cntrct_templ`
        *   `your-gcp-project.your_dataset.ta_p_discount` (This is a target table for `d_ausd_v_ta_p_discount.sql` but also a source for other processes potentially)
        *   `your-gcp-project.your_dataset.ta_disc_zusgf`
        *   `your-gcp-project.your_dataset.ta_p_discount_rr` (This is a target table for `d_ausd_v_ta_p_discount_rr.sql`)
    *   **Crucial:** Define appropriate schemas (column names, data types) for these tables based on the Oracle source.

3.  **Data Ingestion Pipeline for Oracle "Carmen" Data:**
    *   Implement and configure a robust data ingestion pipeline to continuously load data from the legacy Oracle "Carmen" database (`@pcrs1`) into the BigQuery source tables created in step 2. This could involve:
        *   BigQuery Data Transfer Service (DTS) for Oracle.
        *   Cloud Data Fusion.
        *   Custom Dataflow jobs.
        *   Cloud SQL (if Carmen is migrated) with subsequent BigQuery loads.
    *   Verify that data is flowing correctly and is up-to-date.

4.  **BigQuery Table Creation (Audit/Logging):**
    *   Create the following BigQuery tables for job control, logging, and error tracking.
        *   `your-gcp-project.your_dataset.job_control`
            ```sql
            CREATE TABLE `your-gcp-project.your_dataset.job_control` (
                job_id STRING NOT NULL,
                job_name STRING NOT NULL,
                start_time TIMESTAMP NOT NULL,
                end_time TIMESTAMP,
                status STRING NOT NULL,
                processing_date DATE,
                message STRING
            );
            ```
        *   `your-gcp-project.your_dataset.job_log`
            ```sql
            CREATE TABLE `your-gcp-project.your_dataset.job_log` (
                job_id STRING NOT NULL,
                log_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
                log_level STRING NOT NULL,
                message STRING NOT NULL
            );
            ```
        *   `your-gcp-project.your_dataset.job_error_log`
            ```sql
            CREATE TABLE `your-gcp-project.your_dataset.job_error_log` (
                job_id STRING NOT NULL,
                error_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
                script_name STRING,
                error_message STRING NOT NULL
            );
            ```
        *   `your-gcp-project.your_dataset.job_audit` (if specific audit requirements exist beyond `job_control` and `job_log`)

5.  **Deploy BigQuery Stored Procedures:**
    *   Execute the DDL for all generated BigQuery Stored Procedures (`sp_k_ausd_v_ta_p_discount_rr.sql`, `sp_k_ausd_v_ta_p_discount.sql`, `sp_r_ausd_v_ta_p_discount_rr.sql`, `sp_r_ausd_v_ta_p_discount.sql`) in the `your_dataset` dataset.
    *   **Important:** Replace `your-gcp-project` and `your_dataset` placeholders in the SQL files with actual values before deployment.

6.  **IAM Permissions:**
    *   The Airflow service account (typically `service-<project-number>@composer-agent.iam.gserviceaccount.com` or a custom service account) needs the following roles:
        *   `BigQuery Data Editor` (or more granular `BigQuery Data Owner` for `TRUNCATE` operations) on `your-gcp-project.your_dataset` to execute stored procedures and write to tables.
        *   `BigQuery Job User` on `your-gcp-project` to run BigQuery jobs.
        *   `Composer Worker` (usually default for Composer environments).
    *   Ensure the service account used by BigQuery for stored procedure execution has permissions to read from source tables and write to target tables.

7.  **Airflow Connection:**
    *   Verify that the `google_cloud_default` Airflow connection is correctly configured in your Cloud Composer environment. This connection is used by `BigQueryExecuteStoredProcedureOperator`.

8.  **Deploy Airflow DAGs:**
    *   Upload the generated DAG files (`dw_bert_ausd_v_ta_p_discount_rr.py`, `dw_bert_ausd_v_ta_p_discount.py`) to the DAGs folder of your Cloud Composer environment.
    *   **Important:**
        *   Replace `your-gcp-project` and `your_dataset` placeholders in the Python files.
        *   Define the `start_date` and `schedule_interval` for each DAG based on the original UC4 scheduling requirements. The generated DAGs have `start_date=datetime(2023, 1, 1)` and `schedule_interval=None` as placeholders.
        *   Review and configure `default_args` such as `email_on_failure`, `retries`, etc., as per operational standards.

## 5. Known gaps & unresolved references

The following items were flagged during the migration design phase and require further attention:

1.  **Missing `file_complexity` data:** The complexity tier and migration flags were not available for any of the source files. This means that an accurate, automated assessment of migration effort and potential challenges was lacking. Manual review of these files is required to assign complexity and identify specific migration risks.
2.  **Absence of `lineage_edges`:** No direct lineage edges were found between the files, requiring inference of the execution flow from code content. This indicates a potential gap in the automated lineage analysis for this job.
3.  **Incomplete UC4 Workflow Export:** The UC4 designs explicitly state that the provided XML files are not a complete workflow export (`EVNT_TIME`, `JOBP` missing). This means that the scheduling and inter-job dependencies (if any) are not fully captured and will need to be manually defined in Airflow DAGs based on external information. The `schedule` and `start_date` for the Airflow DAGs are placeholders and *must be configured*.
4.  **Oracle DB-Link to Carmen (`@pcrs1`):** This is a critical external dependency. The migration strategy for the Carmen database and its data ingestion into BigQuery must be finalized and implemented. Any on-premises Oracle data sources need to be continuously synchronized with BigQuery or fully migrated. This is a **blocking prerequisite**.
5.  **KornShell Helpers:** The KornShell scripts rely on several helper scripts (e.g., `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`). While the core orchestration and logging have been translated, the full extent of logic within these helpers needs to be thoroughly analyzed. Any remaining specific logic not covered by BigQuery's native capabilities or the current logging framework might need to be reimplemented as BigQuery UDFs, additional stored procedure logic, or potentially within the Airflow DAG if it's environment-specific.
6.  **"Active Jobs Ignored" / "Deactivate Old Active Jobs" logic:** The KornShell scripts contain logic to handle active jobs (ignore or deactivate). This job management logic needs to be carefully translated into the BigQuery stored procedures and Airflow tasks to ensure correct behavior in the new environment. The current `job_control` table provides a basic status, but the specific "active job" handling logic (e.g., preventing concurrent runs, handling stale runs) from the original KornShell scripts needs to be reviewed and potentially enhanced in the BigQuery stored procedures or Airflow DAGs (e.g., using Airflow's `max_active_runs` or custom logic).

## 6. Validation

To validate the successful migration and functionality of the `DW.BERT_AUSD_V_TA_P_DISCOUNT` job, follow these steps:

1.  **Prerequisites:** Ensure all manual steps (Section 4) are completed, especially the data ingestion from Oracle Carmen into BigQuery.

2.  **Trigger Airflow DAGs:**
    *   Manually trigger both `dw_bert_ausd_v_ta_p_discount_rr` and `dw_bert_ausd_v_ta_p_discount` DAGs from the Airflow UI.
    *   Alternatively, wait for their scheduled runs if `schedule_interval` has been configured.

3.  **Monitor Airflow UI:**
    *   Observe the DAG runs in the Airflow UI. All tasks within each DAG should transition to a "success" state.
    *   Check task logs for any errors or unexpected output.

4.  **Verify BigQuery Stored Procedure Execution:**
    *   In the BigQuery UI, navigate to the `your_dataset` dataset.
    *   Query the `job_control` table:
        ```sql
        SELECT * FROM `your-gcp-project.your_dataset.job_control`
        WHERE job_name IN ('DW.BERT_V_TA_P_DISCOUNT_RR', 'DW.BERT_V_TA_P_DISCOUNT')
        ORDER BY start_time DESC;
        ```
        *   **Passing criteria:** The `status` column for the latest runs should be 'COMPLETED'.
    *   Query the `job_log` table:
        ```sql
        SELECT * FROM `your-gcp-project.your_dataset.job_log`
        WHERE job_id IN (SELECT job_id FROM `your-gcp-project.your_dataset.job_control` WHERE job_name IN ('DW.BERT_V_TA_P_DISCOUNT_RR', 'DW.BERT_V_TA_P_DISCOUNT') ORDER BY start_time DESC LIMIT 2)
        ORDER BY log_time;
        ```
        *   **Passing criteria:** Look for 'INFO' messages indicating start and successful completion of `sp_r_*` and `sp_k_*` procedures. There should be no 'ERROR' level messages.
    *   Query the `job_error_log` table:
        ```sql
        SELECT * FROM `your-gcp-project.your_dataset.job_error_log`
        WHERE job_id IN (SELECT job_id FROM `your-gcp-project.your_dataset.job_control` WHERE job_name IN ('DW.BERT_V_TA_P_DISCOUNT_RR', 'DW.BERT_V_TA_P_DISCOUNT') ORDER BY start_time DESC LIMIT 2);
        ```
        *   **Passing criteria:** This table should be empty for successful runs.

5.  **Data Validation:**
    *   **Record Count Check:** Compare the number of rows in the target BigQuery tables (`your-gcp-project.your_dataset.ta_p_discount_rr`, `your-gcp-project.your_dataset.ta_p_discount`) with the corresponding tables in the legacy Oracle environment after a successful run.
        ```sql
        SELECT COUNT(*) FROM `your-gcp-project.your_dataset.ta_p_discount_rr`;
        SELECT COUNT(*) FROM `your-gcp-project.your_dataset.ta_p_discount`;
        ```
    *   **Data Sample Check:** Select a sample of records from the target BigQuery tables and compare them with the corresponding records in the legacy Oracle tables. Focus on key columns and derived values to ensure transformation logic is correct.
        ```sql
        SELECT * FROM `your-gcp-project.your_dataset.ta_p_discount_rr` LIMIT 100;
        SELECT * FROM `your-gcp-project.your_dataset.ta_p_discount` LIMIT 100;
        ```
    *   **Edge Case Testing:** If specific edge cases were identified in the original Oracle SQL, ensure these are handled correctly in BigQuery.

**"Passing" means:**
*   Both Airflow DAGs complete successfully without errors.
*   The `job_control` table shows 'COMPLETED' status for the respective job runs.
*   The `job_log` table shows successful execution messages and no 'ERROR' entries.
*   The `job_error_log` table is empty for the validated runs.
*   The record counts in the target BigQuery tables match the legacy Oracle tables (or are within an acceptable delta if data changes over time).
*   A sample of data from the target BigQuery tables matches the expected output from the legacy system, confirming the transformation logic.

## 7. Rollback procedure

In case of critical issues or unexpected behavior after go-live, the following rollback procedure can be initiated to revert to the legacy system:

1.  **Stop New Runs in GCP:**
    *   In the Airflow UI, unpause both `dw_bert_ausd_v_ta_p_discount_rr` and `dw_bert_ausd_v_ta_p_discount` DAGs to prevent any further execution.

2.  **Reactivate Legacy UC4 Jobs:**
    *   In the UC4/Automic scheduler, reactivate the original `DW.BERT_AUSD_V_TA_P_DISCOUNT_RR.xml` and `DW.BERT_AUSD_V_TA_P_DISCOUNT.xml` jobs.
    *   Verify that the legacy jobs are running as expected and producing correct output in the Oracle environment.

3.  **Data State Assessment (Optional but Recommended):**
    *   If the BigQuery target tables (`ta_p_discount_rr`, `ta_p_discount`) were updated incorrectly, assess the impact. Since these jobs truncate and reload, the impact might be limited to the data generated during the failed GCP run.
    *   If necessary, restore the BigQuery target tables from a previous successful snapshot or re-run the legacy job to populate the Oracle tables, then re-ingest that data into BigQuery if BigQuery is still intended as a reporting layer.

4.  **Isolate and Debug:**
    *   Once the legacy system is operational, thoroughly investigate the root cause of the failure in the GCP environment. This may involve reviewing Airflow logs, BigQuery job logs, and the data in the BigQuery tables.

5.  **Cleanup (Post-Rollback):**
    *   After a successful rollback and stabilization of the legacy system, the BigQuery stored procedures and Airflow DAGs can be temporarily removed or disabled in GCP until the issues are resolved and a re-migration or fix is ready.
    *   Do NOT delete the BigQuery source tables or the data ingestion pipeline, as these are prerequisites for any future migration attempts.