# MIGRATION_NOTES.md

## 1. Summary

The KornShell script `vobs/dw_source/isrpt/isbert/install_save/k_ausd_bp_ta_tarifoption.ksh` has been migrated. This script, originally responsible for orchestrating a data processing job, including parameter parsing, date validation, and executing a core SQL script (`d_ausd_bp_ta_tarifoption.sql`) against the `PoolBasisprodukt` table, has been re-platformed.

The migration target is Google Cloud Platform, specifically leveraging **Google BigQuery** for data processing and transformation, and **Cloud Composer (Apache Airflow)** for job orchestration and scheduling. The original shell script's control flow, parameter handling, and error logging have been translated into a BigQuery SQL stored procedure, while the scheduling aspect is managed by a Cloud Composer DAG.

## 2. Generated artifacts

The migration process generated the following artifacts:

*   **`bq_ddl/job_log.sql`**
    *   **Role:** This SQL script defines the Data Definition Language (DDL) for the `job_log` table in BigQuery. This table serves as the centralized logging mechanism for the migrated job, capturing execution status, error messages, and record counts, replacing the temporary files and commented-out logging calls from the original KornShell script.
*   **`bq_ddl/poolbasisprodukt.sql`**
    *   **Role:** This SQL script provides a placeholder DDL for the `PoolBasisprodukt` table in BigQuery. This table is the primary data source/target for the core transformation logic. Its schema needs to be accurately defined based on the legacy source system's `PoolBasisprodukt` table.
*   **`bq_sprocs/r_ausd_bp_ta_tarifoption.sql`**
    *   **Role:** This BigQuery SQL stored procedure encapsulates the core orchestration logic of the original `k_ausd_bp_ta_tarifoption.ksh` script. It handles parameter reception, validation (job ID, entry number, date format), date derivation, error logging to `job_log`, and the execution of the data transformation logic (which will be derived from `d_ausd_bp_ta_tarifoption.sql`). It also captures and logs record counts.
*   **`airflow_dags/r_ausd_bp_ta_tarifoption_dag.py`**
    *   **Role:** This Python script defines an Apache Airflow DAG for Cloud Composer. It is responsible for scheduling and invoking the `r_ausd_bp_ta_tarifoption` BigQuery stored procedure, passing the necessary parameters. This DAG replaces the original shell script's role as the entry point for job execution.

## 3. Key design decisions

*   **BigQuery Stored Procedure for Orchestration:** The decision to translate the KornShell script's orchestration logic into a BigQuery stored procedure (`r_ausd_bp_ta_tarifoption`) was made to leverage BigQuery's native capabilities for data processing and to keep the entire data pipeline within the BigQuery ecosystem as much as possible. This eliminates the need for external compute instances for orchestration and allows for direct execution of BigQuery SQL.
*   **Cloud Composer for Scheduling:** Cloud Composer (Airflow) was chosen as the orchestration layer to replace the legacy scheduler. Airflow provides robust scheduling, dependency management, monitoring, and logging capabilities, which are superior to simple shell-based cron jobs or custom schedulers. It also integrates seamlessly with BigQuery.
*   **Consolidated Logging:** A dedicated `job_log` BigQuery table was introduced to centralize all job execution logs, status messages, and error information. This replaces the disparate logging mechanisms (temporary files, `DWMSG_MeldeFehler` calls) of the original script, providing a structured and queryable audit trail.
*   **Native BigQuery Functions:** All custom shell utilities for date handling (`h_alis_date.ksh`, `gestern.ksh`) and parameter validation (`h_alis_parameter.ksh`) have been replaced with equivalent BigQuery SQL functions (e.g., `CURRENT_DATE()`, `DATE_SUB()`, `SAFE.PARSE_DATE()`, `IF` statements). This reduces external dependencies and simplifies the code.
*   **Direct SQL Execution:** The reliance on `h_alis_sqlplus.ksh` and SQL*Plus to execute `d_ausd_bp_ta_tarifoption.sql` has been eliminated. The core transformation logic will be directly embedded or called within the BigQuery stored procedure, removing the need for an external database client.

**Notable Trade-offs:**

*   **Loss of Shell Flexibility:** The migration to BigQuery SQL and Airflow means losing the direct flexibility of shell scripting for file system operations, arbitrary command execution, or integration with other non-GCP specific tools. Any such requirements would need to be re-engineered using GCP services (e.g., Cloud Storage, Cloud Functions).
*   **`d_ausd_bp_ta_tarifoption.sql` Analysis Required:** The core data transformation logic from `d_ausd_bp_ta_tarifoption.sql` was not provided and is represented as a placeholder. This requires significant follow-up analysis and conversion effort, which is a critical dependency for the completeness of this migration.
*   **Commented Code Assumption:** The commented-out sections in the original script (e.g., `sed`, `sort`, `join` commands, job management functions) were assumed to be inactive and were not migrated. If these represent latent requirements, they would need to be re-evaluated and implemented in BigQuery SQL or other GCP services.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps are required:

1.  **BigQuery Dataset Creation:**
    *   Ensure the target BigQuery dataset (e.g., `your_bigquery_dataset` within `your-gcp-project-id`) exists. If not, create it.
2.  **IAM Permissions:**
    *   The service account used by Cloud Composer (or the user deploying/running the stored procedure) must have appropriate IAM roles:
        *   `BigQuery Data Editor` on the target dataset to create/update tables and run jobs.
        *   `BigQuery Job User` to run BigQuery jobs.
        *   `BigQuery Data Viewer` on any source tables.
        *   `Composer Worker` and `Composer User` roles for the Cloud Composer environment.
3.  **BigQuery Table DDL Deployment:**
    *   Execute `bq_ddl/job_log.sql` to create the `job_log` table in the target BigQuery dataset.
    *   **Crucially, complete and execute `bq_ddl/poolbasisprodukt.sql` (and any other related source/target tables) with the correct schema derived from the legacy system.**
4.  **Data Migration:**
    *   Migrate the actual data from the legacy `PoolBasisprodukt` table (and any other relevant tables) into their respective BigQuery tables. This might involve using tools like BigQuery Data Transfer Service, `bq load` commands, or custom ETL pipelines.
5.  **BigQuery Stored Procedure Deployment:**
    *   Deploy the `bq_sprocs/r_ausd_bp_ta_tarifoption.sql` stored procedure to the target BigQuery dataset. This can be done via the BigQuery UI, `bq` command-line tool, or a CI/CD pipeline.
    *   **Integrate the fully translated BigQuery SQL from `d_ausd_bp_ta_tarifoption.sql` into this stored procedure.** This is a critical step.
6.  **Cloud Composer Environment Setup:**
    *   Ensure a Cloud Composer environment is provisioned and running.
    *   Verify the `google_cloud_default` Airflow connection is configured correctly to connect to your GCP project.
7.  **Airflow DAG Deployment:**
    *   Upload `airflow_dags/r_ausd_bp_ta_tarifoption_dag.py` to the DAGs folder of your Cloud Composer environment.
    *   Adjust the `project_id`, `dataset_id`, and parameter values (especially `p_Stichtag` if it needs a different macro or static value) within the DAG to match your environment and requirements.
8.  **Parameter Configuration:**
    *   Review and configure the default parameter values (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`) in the Airflow DAG to align with the expected runtime inputs.

## 5. Known gaps & unresolved references

The following items are identified as gaps, unresolved references, or require further attention:

*   **`d_ausd_bp_ta_tarifoption.sql` Content Migration (B4 Item):** The most significant gap is the actual SQL logic within `d_ausd_bp_ta_tarifoption.sql`. This content was not provided and is critical for the job's functionality. It must be thoroughly analyzed, translated from its original SQL dialect (likely Oracle) to BigQuery SQL, and integrated into the `r_ausd_bp_ta_tarifoption` stored procedure. This is a **Blocker (B4)** item for full functionality.
*   **`PoolBasisprodukt` Table Schema:** The DDL for `PoolBasisprodukt` is a placeholder. The complete and accurate schema (column names, data types, partitioning/clustering keys) must be derived from the source system and implemented in BigQuery.
*   **Commented-out Code Confirmation:** The original script contained commented-out sections (e.g., `sed`, `sort`, `join` commands, `FOSJobErzeugeEintrag` calls). It is assumed these are inactive. **Confirmation from the business owner is required** to ensure no critical logic was inadvertently omitted. If active, these would require re-engineering for BigQuery.
*   **Custom Error Number Mapping:** The original script uses a custom `ErrNr` system. While the `job_log` table captures `error_nr`, a standardized mapping to BigQuery/GCP error handling best practices or a more descriptive error message system might be beneficial for future maintainability and integration with GCP monitoring.
*   **Missing Complexity/Automation Data:** The absence of `file_complexity` and `automation_rate` information for the source script means the initial assessment of migration effort and automation potential was limited. This implies a higher degree of manual validation and potential for unforeseen complexities during the `d_ausd_bp_ta_tarifoption.sql` migration.
*   **Dynamic Parameter Handling:** The original script's "Bitte ueber Rahmenscript aufrufen" message suggests it's part of a larger framework. While Cloud Composer replaces the "Rahmenscript," any dynamic generation or derivation of `p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, or `p_wiederanlaufWert` from an upstream process needs to be replicated in the Airflow DAG or its upstream tasks.

## 6. Validation

Validation ensures the migrated job functions correctly and produces accurate results.

**How to run the tests:**

1.  **Manual BigQuery Stored Procedure Execution:**
    *   Open the BigQuery UI.
    *   Navigate to the `r_ausd_bp_ta_tarifoption` stored procedure.
    *   Click "Run" and provide sample input parameters (e.g., `p_JobKennung='TEST_JOB'`, `p_EintragsNr='1'`, `p_Stichtag='01012023'`, `p_wiederanlaufWert='0'`).
    *   Observe the execution status and check the `job_log` table for entries.
2.  **Cloud Composer DAG Trigger:**
    *   Access the Airflow UI for your Cloud Composer environment.
    *   Locate the `r_ausd_bp_ta_tarifoption_dag`.
    *   Manually trigger the DAG.
    *   Monitor the DAG run in the Airflow UI for task success/failure.
    *   Review task logs for any errors or warnings.
    *   Check the `job_log` table in BigQuery for entries corresponding to the DAG run.

**What "passing" means:**

*   **Successful Execution:** Both manual stored procedure runs and DAG runs complete without errors in BigQuery or Airflow logs.
*   **Correct Parameter Handling:** The stored procedure correctly receives and validates all input parameters. Invalid parameters should result in an error logged to `job_log` and the procedure exiting gracefully.
*   **Accurate Date Logic:** The date derivation (`v_datum_heute`, `v_datum_gestern`) and validation (`v_stichtag_date`) function as expected, matching the logic of the original `gestern.ksh` and `DWDate_Datum_Check`.
*   **Data Transformation Accuracy:** The core transformation logic (once `d_ausd_bp_ta_tarifoption.sql` is fully migrated) produces the same output data in the target BigQuery tables as the original script did in the legacy environment, given the same input data. This requires a data comparison between source and target systems.
*   **Correct Record Counts:** The `record_count` logged in the `job_log` table accurately reflects the number of rows processed or affected by the transformation.
*   **Comprehensive Logging:** The `job_log` table contains appropriate entries for job start, status updates, record counts, and any error conditions, providing a clear audit trail.
*   **No Unexpected Errors:** No unexpected BigQuery errors, Airflow task failures, or resource exhaustion issues are observed.

## 7. Rollback procedure

In case of critical issues or failure during go-live, the following rollback procedure can be initiated:

1.  **Disable New Job:**
    *   **Cloud Composer:** Pause or delete the `r_ausd_bp_ta_tarifoption_dag` in the Airflow UI to prevent further executions.
    *   **BigQuery Stored Procedure:** If necessary, revoke execution permissions or drop the `r_ausd_bp_ta_tarifoption` stored procedure to ensure it cannot be called.
2.  **Revert Scheduling:**
    *   Re-enable the original scheduling mechanism (e.g., cron job) for `k_ausd_bp_ta_tarifoption.ksh` in the legacy environment.
3.  **Data Reversion (if applicable):**
    *   If the migrated job made irreversible changes to target BigQuery tables, a data rollback strategy must be employed. This could involve:
        *   Restoring target tables from a previous snapshot or backup (if available).
        *   Running a reverse transformation or data correction script.
        *   **Note:** This step requires careful planning and depends heavily on the nature of the data transformations and the availability of recovery points. For jobs that append or create new data, this might be less critical than for jobs that update or delete existing data.
4.  **Verify Legacy System:**
    *   Confirm that the original `k_ausd_bp_ta_tarifoption.ksh` script is running successfully and processing data as expected in the legacy environment.
5.  **Analysis and Remediation:**
    *   Analyze the root cause of the failure in the migrated system. Address the identified issues (e.g., code bugs, configuration errors, missing data, performance bottlenecks) before attempting another deployment.