# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the `DW.BERT_AUSD_V_TA_CNTRCT_TEMPL` job. This job, originally responsible for mirroring Carmen contract templates from Oracle to another Oracle target table, orchestrated by UC4 and KornShell scripts, has been re-implemented.

The migration targets Google Cloud Platform (GCP), utilizing:
*   **BigQuery** for data storage and transformation.
*   **Apache Airflow on Cloud Composer** for orchestration.

The original workflow involved extracting contract template data and descriptions, applying filtering logic, and loading into `sof$ta_cntrct_templ`. This entire process has been translated into BigQuery SQL and orchestrated via an Airflow DAG.

## 2. Generated artifacts

The migration produced the following artifacts:

*   **`bigquery/ddl/sof_ta_cntrct_templ.sql`**
    *   **Role:** BigQuery Data Definition Language (DDL) script to create the target table `sof_ta_cntrct_templ`. This table mirrors the structure of the legacy Oracle `sof$ta_cntrct_templ` table, where the processed contract template data will be stored.
*   **`bigquery/ddl/job_log.sql`**
    *   **Role:** BigQuery DDL script to create a logging table (`job_log`). This table is designed to capture start and end times, status, and general messages for job executions, replacing parts of the legacy shell script's custom logging.
*   **`bigquery/ddl/job_error_log.sql`**
    *   **Role:** BigQuery DDL script to create an error logging table (`job_error_log`). This table will store details about any errors encountered during job execution, including error codes, messages, and arguments, replacing the error handling mechanisms in the legacy KornShell scripts.
*   **`bigquery/ddl/job_result.sql`**
    *   **Role:** BigQuery DDL script to create a result logging table (`job_result`). This table records key metrics such as records processed and final status upon job completion.
*   **`bigquery/ddl/job_status.sql`**
    *   **Role:** BigQuery DDL script to create a status logging table (`job_status`). This table tracks the overall status of the job at various points, providing a high-level overview of its execution.
*   **`bigquery/sql/d_ausd_v_ta_cntrct_templ_transform.sql`**
    *   **Role:** BigQuery SQL script containing the core data transformation logic. This script is a direct translation of the original Oracle SQL*Plus script (`d_ausd_v_ta_cntrct_templ.sql`), responsible for determining the processing date, truncating the target table, and inserting filtered and joined data from source tables into `sof_ta_cntrct_templ`.
*   **`bigquery/stored_procedures/r_ausd_v_ta_cntrct_templ.sql`**
    *   **Role:** BigQuery Stored Procedure that encapsulates the orchestration, parameter handling, and error management logic previously found in the KornShell wrapper (`r_ausd_v_ta_cntrct_templ.ksh`) and control (`k_ausd_v_ta_cntrct_templ.ksh`) scripts. This stored procedure calls the core transformation logic (embedded within it) and handles logging to the BigQuery audit tables.
*   **`airflow/dags/dw_bert_ausd_v_ta_cntrct_templ.py`**
    *   **Role:** An Apache Airflow DAG (Directed Acyclic Graph) written in Python. This DAG replaces the UC4 job and orchestrates the execution of the BigQuery Stored Procedure. It defines the job's schedule (if any), dependencies, and the BigQuery operator responsible for invoking the stored procedure.

## 3. Key design decisions

The migration strategy involved several key design decisions to leverage GCP capabilities and address the limitations of the legacy system:

*   **Migration to BigQuery for Data Storage and Transformation:**
    *   **Why:** BigQuery offers a fully managed, serverless, and highly scalable data warehouse solution. Its columnar storage and distributed query engine are ideal for analytical workloads and large datasets, significantly outperforming traditional Oracle databases for such tasks. It simplifies infrastructure management and provides a powerful SQL interface for transformations.
    *   **Approach:** All source and target Oracle tables (`isbert_schema.dwtk_meldungen`, `cds$ta_cntrct_template`, `cds$ta_care_description`, `sof$ta_cntrct_templ`) are replicated as BigQuery tables. The Oracle SQL*Plus transformation logic is directly translated into BigQuery Standard SQL.
    *   **Trade-offs:** Requires a separate data ingestion pipeline for source data (e.g., from Carmen via `v_carmen` DB-link) into BigQuery. Potential differences in SQL dialect and function behavior need careful translation and testing.

*   **Apache Airflow on Cloud Composer for Orchestration:**
    *   **Why:** Cloud Composer provides a managed Airflow service, offering robust scheduling, monitoring, and workflow management capabilities. It's a modern, cloud-native replacement for UC4, allowing for complex DAG definitions in Python, better version control, and integration with other GCP services.
    *   **Approach:** The UC4 job is replaced by an Airflow DAG. The KornShell scripts' orchestration logic (parameter parsing, environment setup, error handling, logging) is consolidated into a BigQuery Stored Procedure, which is then invoked by a `BigQueryExecuteQueryOperator` in the Airflow DAG.
    *   **Trade-offs:** Requires learning Airflow/Python for DAG development. The direct shell script execution flexibility is lost, requiring translation of shell logic into BigQuery SQL or Python.

*   **Consolidation of KornShell Logic into BigQuery Stored Procedures:**
    *   **Why:** The original job had multiple KornShell scripts (`r_ausd_v_ta_cntrct_templ.ksh`, `k_ausd_v_ta_cntrct_templ.ksh`) handling environment setup, parameter parsing, and error logging, which then invoked an SQL*Plus script. Consolidating this orchestration logic into a single BigQuery Stored Procedure (`r_ausd_v_ta_cntrct_templ`) simplifies the workflow, reduces inter-process communication overhead, and keeps the logic closer to the data. It also allows for BigQuery's native error handling (`TRY...CATCH`) and logging capabilities.
    *   **Approach:** The `r_ausd_v_ta_cntrct_templ` stored procedure handles parameter validation, job logging, execution of the core transformation SQL, and error reporting.
    *   **Trade-offs:** BigQuery Stored Procedures, while powerful, can be less flexible than shell scripts for complex file system operations or external command execution (though not relevant for this job).

*   **Custom Logging and Error Handling in BigQuery:**
    *   **Why:** The legacy system relied on custom `DWMSG_*` utilities and shell script `trap` commands for logging and error handling. To maintain observability and traceability in the new environment, dedicated BigQuery tables (`job_log`, `job_error_log`, `job_result`, `job_status`) are created.
    *   **Approach:** The BigQuery Stored Procedure (`r_ausd_v_ta_cntrct_templ`) is designed to write entries to these tables at various stages (start, end, error) and utilize BigQuery's `EXCEPTION WHEN ERROR` blocks for robust error capture.
    *   **Trade-offs:** Requires explicit `INSERT` statements for logging, rather than implicit system logging. The full functionality of all `DWMSG_*` utilities needs careful mapping.

## 4. Manual steps before go-live

Before the migrated job can go live, several manual or one-time setup steps are required:

1.  **GCP Project and Dataset Setup:**
    *   Ensure a GCP project is provisioned.
    *   Create the necessary BigQuery datasets:
        *   `your_project.your_dataset` (e.g., `dw_bert_staging`)
        *   `your_project.isbert_schema` (for `dwtk_meldungen`)
    *   Replace all `your_project` and `your_dataset` placeholders in the generated code with actual project and dataset IDs.

2.  **BigQuery Table Creation (DDL):**
    *   Execute the DDL scripts to create the target and logging tables in BigQuery:
        *   `bigquery/ddl/sof_ta_cntrct_templ.sql`
        *   `bigquery/ddl/job_log.sql`
        *   `bigquery/ddl/job_error_log.sql`
        *   `bigquery/ddl/job_result.sql`
        *   `bigquery/ddl/job_status.sql`

3.  **BigQuery Stored Procedure Deployment:**
    *   Execute the DDL script to create the orchestration stored procedure:
        *   `bigquery/stored_procedures/r_ausd_v_ta_cntrct_templ.sql`

4.  **IAM and Permissions:**
    *   **Service Account:** Create a dedicated GCP service account for the Airflow DAG (if not using the default Composer service account).
    *   **BigQuery Permissions:** Grant the Airflow service account (or Composer service account) the necessary BigQuery roles:
        *   `BigQuery Data Editor` on `your_project.your_dataset` (for `sof_ta_cntrct_templ`, `job_log`, `job_error_log`, `job_result`, `job_status`).
        *   `BigQuery Data Viewer` on `your_project.isbert_schema` (for `dwtk_meldungen`) and `your_project.your_dataset` (for `cds_ta_cntrct_template`, `cds_ta_care_description`).
        *   `BigQuery Job User` for running queries and stored procedures.

5.  **Airflow Connection Strings:**
    *   Ensure the `google_cloud_default` connection (or a custom GCP connection) is properly configured in your Cloud Composer environment. This connection is used by the `BigQueryExecuteQueryOperator`.

6.  **Source Data Ingestion:**
    *   **Crucial Step:** Implement and configure the data ingestion pipelines to bring data from the source systems (Oracle, Carmen) into BigQuery. This is a prerequisite for the job to run successfully.
        *   `isbert_schema.dwtk_meldungen` -> `your_project.isbert_schema.dwtk_meldungen`
        *   `cds$ta_cntrct_template` (from Carmen via `v_carmen`) -> `your_project.your_dataset.cds_ta_cntrct_template`
        *   `cds$ta_care_description` (from Carmen via `v_carmen`) -> `your_project.your_dataset.cds_ta_care_description`
    *   These ingestion jobs must run *before* `dw_bert_ausd_v_ta_cntrct_templ` to ensure fresh source data is available.

7.  **Airflow DAG Deployment:**
    *   Upload the `airflow/dags/dw_bert_ausd_v_ta_cntrct_templ.py` file to the DAGs folder of your Cloud Composer environment.

8.  **Scheduling:**
    *   Review and set the `schedule_interval` in the Airflow DAG (`dw_bert_ausd_v_ta_cntrct_templ.py`) to match the desired execution frequency (e.g., daily, hourly, or `None` for manual/event-driven triggers).

## 5. Known gaps & unresolved references

The following items were identified during the migration design and code generation as requiring further analysis, follow-up, or representing known limitations:

*   **Detailed `DWMSG_*` Utilities Re-implementation:** The provided BigQuery logging tables (`job_log`, `job_error_log`, `job_result`, `job_status`) offer a basic replacement for the legacy `DWMSG_*` utilities. However, the exact logic and full functionality of all `DWMSG_*` utilities (e.g., `DWMSG_MeldeFehler`, `DWMSG_ErmittleNr`, `DWMSG_Logdateiname`, `DWMSG_ErzeugeEintrag`, `DWMSG_SetzeStichtagInfo`, `DWMSG_Fehlerbehandlung`, `DWMSG_SetzeStatusOK`) need to be thoroughly analyzed. If they contain complex business logic or specific reporting requirements, the current BigQuery logging might need enhancement.
*   **Shell Utilities (`f_alis_msgerr.ksh`, `h_alis_job.ksh`, etc.):** Similar to `DWMSG_*`, the helper KornShell scripts sourced by the original job (`f_alis_msgerr.ksh`, `h_alis_job.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`) were not fully translated. While basic parameter parsing and date handling are covered, any intricate logic within these scripts (e.g., specific environment variable setups, complex date calculations, or custom error formatting) might require explicit re-implementation in the BigQuery Stored Procedure or the Airflow DAG.
*   **Full Parameter Validation in Stored Procedure:** The `r_ausd_v_ta_cntrct_templ` stored procedure includes a basic parameter check for `p_JobKennung`. A comprehensive analysis of all parameters and their validation rules from the original KornShell scripts is needed to ensure full functional parity.
*   **`v_carmen` DB-Link / Carmen Data Ingestion Pipeline:** The design document explicitly states that the `v_carmen @pcrs1` DB-link implies a separate data ingestion pipeline is required to bring `cds$ta_cntrct_template` and `cds$ta_care_description` data from the Carmen database into BigQuery. This ingestion pipeline is a critical prerequisite and is *not* part of the current job's migration scope. Its implementation and operationalization must be completed independently.
*   **Source Table Schemas:** The exact column names and data types for `isbert_schema.dwtk_meldungen`, `cds$ta_cntrct_template`, and `cds$ta_care_description` were assumed based on the SQL logic. A definitive schema definition from the source Oracle system is required to ensure correct BigQuery table creation and data type mapping during ingestion.
*   **Placeholder Replacement:** All placeholders like `your_project`, `your_dataset` must be replaced with actual GCP resource names before deployment.
*   **`TRUNCATE` behavior:** The Oracle `TRUNCATE` command is replaced by BigQuery's `TRUNCATE TABLE`. While functionally similar for this use case, it's worth noting that BigQuery's `TRUNCATE` is a DDL operation and has different transactional properties than Oracle's.

## 6. Validation

Validation ensures the migrated job functions correctly and produces accurate results compared to the legacy system.

**How to run the tests:**

1.  **Ensure Source Data Ingestion:** Verify that the BigQuery tables `your_project.isbert_schema.dwtk_meldungen`, `your_project.your_dataset.cds_ta_cntrct_template`, and `your_project.your_dataset.cds_ta_care_description` contain up-to-date and accurate data from their respective source systems.
2.  **Manual BigQuery Stored Procedure Execution:**
    *   Open the BigQuery console.
    *   Execute the `r_ausd_v_ta_cntrct_templ` stored procedure directly:
        ```sql
        CALL `your_project.your_dataset.r_ausd_v_ta_cntrct_templ`(
            p_JobKennung => 'BERT_AUSD_V_TA_CNTRCT_TEMPL_TEST',
            p_EintragsNr => 999 -- Use a distinct entry number for testing
        );
        ```
    *   Monitor the BigQuery job history for completion and any errors.
3.  **Airflow DAG Trigger:**
    *   Access the Airflow UI in Cloud Composer.
    *   Locate the `dw_bert_ausd_v_ta_cntrct_templ` DAG.
    *   Manually trigger the DAG.
    *   Monitor the DAG run in the Airflow UI for task success/failure and check logs for details.
4.  **BigQuery Logging Tables:**
    *   Query the `job_log`, `job_error_log`, `job_result`, and `job_status` tables in BigQuery to verify that job execution details, status, and any errors are correctly recorded.

**What "passing" means:**

A successful migration is validated when:

1.  **Functional Equivalence:** The `sof_ta_cntrct_templ` table in BigQuery contains the exact same data (same number of rows, same values for each column) as the `sof$ta_cntrct_templ` table in the legacy Oracle system after a comparable run.
    *   Perform a row count comparison: `SELECT COUNT(*) FROM your_project.your_dataset.sof_ta_cntrct_templ;`
    *   Perform a data comparison (e.g., checksums, row-by-row comparison for a sample, or full data diff if feasible) between the BigQuery target table and the legacy Oracle target table.
2.  **Job Completion Status:**
    *   The Airflow DAG run completes successfully without any failed tasks.
    *   The `job_status` table in BigQuery shows a `COMPLETED` status for the corresponding `job_id`.
    *   The `job_result` table shows `SUCCESS` and an accurate `records_processed` count.
3.  **Error Handling:** If an intentional error condition is simulated (e.g., missing source data), the job should gracefully fail, log the error in `job_error_log`, and the Airflow task should reflect the failure.
4.  **Performance:** The job completes within acceptable performance thresholds, ideally faster than the legacy system.

## 7. Rollback procedure

In case of issues or critical failures after go-live, the following rollback procedure can be initiated to revert to the legacy system:

1.  **Disable New Job:**
    *   In the Airflow UI, pause or delete the `dw_bert_ausd_v_ta_cntrct_templ` DAG to prevent further executions.
    *   (Optional) If the BigQuery Stored Procedure was called directly, ensure no external processes are invoking it.

2.  **Re-enable Legacy Job:**
    *   Re-enable the original `DW.BERT_AUSD_V_TA_CNTRCT_TEMPL` job in UC4.
    *   Verify that the legacy job can run successfully and populate the Oracle `sof$ta_cntrct_templ` table.

3.  **Data Consistency (if necessary):**
    *   If the BigQuery target table (`sof_ta_cntrct_templ`) was truncated and repopulated, and the rollback is due to data integrity issues in BigQuery, the data in the legacy Oracle `sof$ta_cntrct_templ` should remain unaffected as it's a separate system.
    *   If the rollback is due to issues with the *source data ingestion* into BigQuery, then the legacy Oracle job will continue to use its direct Oracle sources, unaffected.

4.  **Cleanup (Optional, Post-Rollback):**
    *   Once the legacy system is confirmed to be fully operational, the BigQuery tables, stored procedures, and Airflow DAG related to the migrated job can be removed from GCP, if desired, to avoid resource consumption. This step is typically performed after a successful rollback and root cause analysis.