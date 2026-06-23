# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the `k_ausd_adressen.ksh` KornShell script and its associated Oracle SQL processing. The original job orchestrated the preparation and execution of an address-related data processing workflow, including parameter validation, date calculations, and the execution of `d_ausd_adressen.sql`.

The job has been re-platformed to Google Cloud Platform, leveraging **Cloud Composer (Apache Airflow)** for orchestration and **Google BigQuery** for all data processing and storage. The KornShell script's logic has been re-implemented in Python, and the Oracle SQL script has been converted to BigQuery Standard SQL.

## 2. Generated Artifacts

The migration process generated the following files:

*   **`dags/k_ausd_adressen_utils.py`**
    *   **Role:** A Python utility module containing functions that replicate core logic from the original KornShell utility scripts. This includes `validate_date` (from `h_alis_date.ksh`), `calculate_yesterday_today` (from `gestern.ksh`), `pruefe_parameter_gesetzt` (from `h_alis_parameter.ksh`), and a placeholder for `log_error` (from `f_alis_msgerr.ksh`). These functions are designed to be imported and used by the Airflow DAG.

*   **`dags/d_ausd_adressen.sql.bq`**
    *   **Role:** The converted SQL script for BigQuery. This file contains the BigQuery Standard SQL equivalent of the original `d_ausd_adressen.sql` (Oracle). It handles data type mapping, function conversion, and syntax adjustments for BigQuery. It uses Jinja templating to accept parameters like `v_stichtag`, `source_dataset`, and `target_dataset` from the Airflow DAG. The script includes `TRUNCATE` statements for target tables and `INSERT INTO` statements for data transformations.

*   **`dags/k_ausd_adressen_dag.py`**
    *   **Role:** The main Apache Airflow DAG definition. This Python script orchestrates the entire workflow. It defines the sequence of tasks, including parameter parsing and validation, date calculation, execution of the BigQuery SQL script using `BigQueryExecuteQueryOperator`, and logging of processed record counts. It leverages the functions from `k_ausd_adressen_utils.py` and passes parameters to the BigQuery SQL script via Airflow's `params` and Jinja templating.

## 3. Key Design Decisions

*   **Orchestration with Cloud Composer (Apache Airflow):** Airflow was chosen to replace the KornShell control script due to its robust capabilities for defining, scheduling, and monitoring complex data workflows. It provides a managed service on GCP, reducing operational overhead.
*   **Data Processing with BigQuery:** BigQuery was selected as the target data warehouse for its scalability, performance, and cost-effectiveness for analytical workloads. This necessitated the conversion of Oracle SQL to BigQuery Standard SQL.
*   **Python for Script Logic:** All KornShell-specific logic (parameter parsing, date calculations, error handling) was re-implemented in Python. This aligns with Airflow's native language and allows for better modularity, testability, and integration with GCP services.
*   **Jinja Templating for SQL Parameters:** Instead of shell variables, BigQuery SQL scripts (`d_ausd_adressen.sql.bq`) now use Jinja templating (`{{ params.param_name }}`) to receive dynamic values (e.g., `p_stichtag`, `source_dataset`, `target_dataset`) from the Airflow DAG. This ensures secure and flexible parameter passing.
*   **Elimination of Temporary Files:** The original script used a temporary file (`$DW_DIR_UTL/bert_k_ausd_adressen_$$.tmp`) to capture record counts. In the migrated solution, this is replaced by directly querying the target BigQuery table (`sof_ta_e_reach_gp`) after the SQL execution, leveraging BigQuery's capabilities and eliminating file system dependencies.
*   **Explicit `TRUNCATE` for Intermediate Tables:** The `d_ausd_adressen.sql.bq` script includes explicit `TRUNCATE TABLE` statements at the beginning for all intermediate and final output tables. This mirrors the behavior of the original Oracle script, which implied these tables were persistent and needed to be cleared before each run.
*   **Handling of Commented-Out Job Table Logic:** The original script contained commented-out functionality for managing job table entries (`FOSJobDeaktivate`, `FOSJobErzeugeEintrag`). This logic was not re-implemented in the generated code but noted as an optional future enhancement, requiring explicit business confirmation if needed.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps are required:

1.  **BigQuery Dataset Creation:**
    *   Create the BigQuery datasets specified in the DAG parameters:
        *   `your-gcp-project.your_source_dataset` (e.g., `project_id.dw_source_isrpt_isbert`)
        *   `your-gcp-project.your_target_dataset` (e.g., `project_id.dw_target_isrpt_isbert`)
    *   Ensure these datasets are in the correct GCP project and region.

2.  **BigQuery Table DDL Creation:**
    *   Based on the schema of the original Oracle tables and the `d_ausd_adressen.sql.bq` script, create the necessary BigQuery tables in both the `source_dataset` and `target_dataset`. This includes:
        *   Source tables: `cds_ta_bp_ref`, `cds_ta_inv_definition`, `glv_ta_country`, `glv_ta_description`, `bpd_ta_reachability`, `bpd_ta_business_partner`.
        *   Target/Intermediate tables: `sof_ta_bp_ref_gp`, `sof_ta_bp_ref_re`, `sof_ta_bp_ref_ev`, `sof_ta_bp_ref_dn`, `sof_ta_bp_ref_gp_nodp`, `sof_ta_bp_ref_re_nodp`, `sof_ta_bp_ref_ev_nodp`, `sof_ta_bp_ref_dn_nodp`, `sof_ta_reachability`, `sof_ta_business_pt`, `sof_ta_country`, `sof_ta_country_desc`, `sof_ta_laender_kng`, `sof_ta_e_reach_gp`, `sof_ta_e_reach_re`, `sof_ta_e_reach_dn`, `sof_ta_e_reach_ev`, `sof_ta_e_business_gp`, `sof_ta_e_business_re`, `sof_ta_e_business_dn`, `sof_ta_e_business_ev`, `sof_ta_e_regulierer`.
    *   Pay close attention to data types, nullability, and partitioning/clustering strategies for optimal BigQuery performance.

3.  **Initial Data Loading:**
    *   Load the historical and current data from the original Oracle source tables into their corresponding BigQuery source tables (e.g., `cds_ta_bp_ref` in Oracle to `your_source_dataset.cds_ta_bp_ref` in BigQuery). This can be done using various GCP data migration tools (e.g., Database Migration Service, Dataflow, or custom scripts).

4.  **IAM/Permissions:**
    *   Ensure the Cloud Composer service account has the necessary BigQuery roles:
        *   `BigQuery Data Editor` on the `target_dataset` (to write data).
        *   `BigQuery Data Viewer` on the `source_dataset` (to read data).
        *   `BigQuery Job User` (to run BigQuery jobs).
    *   If the datasets are in different projects, cross-project permissions must be configured.

5.  **Airflow GCP Connection:**
    *   Verify or create an Airflow GCP Connection ID (default `google_cloud_default`) in your Cloud Composer environment. This connection should use the service account identified in the IAM step.

6.  **Scheduling:**
    *   Configure the desired schedule for the `k_ausd_adressen_dag` in Airflow. The original script's scheduling mechanism should be replicated here.

7.  **Secrets Management (if applicable):**
    *   If any sensitive parameters (e.g., API keys, non-GCP credentials) were part of the original environment or are introduced in the migration, ensure they are securely stored and accessed (e.g., using Google Secret Manager and Airflow connections/variables). Currently, no explicit secrets are identified beyond GCP connection.

## 5. Known Gaps & Unresolved References

The following items were identified as gaps or risks during the design and generation phase and require further attention:

*   **`d_ausd_adressen.sql` Detailed Analysis:** While a conversion to BigQuery SQL has been performed, a thorough review of the original Oracle `d_ausd_adressen.sql` content is crucial to ensure all business logic, edge cases, and performance considerations are accurately reflected in the BigQuery version. This includes verifying data type mappings and function equivalences.
*   **`starteSQLSkript` Implementation Details:** The exact behavior of the original `starteSQLSkript` (e.g., transaction management, error handling, output capture) was inferred. The `BigQueryExecuteQueryOperator` handles basic execution and error reporting, but any specific nuances of `starteSQLSkript` (e.g., custom retry logic, specific output parsing) would need to be explicitly added to the DAG if they are critical.
*   **Job Table Management Logic:** The `FOSJobDeaktivate` and `FOSJobErzeugeEintrag` functions were commented out in the original script. It is **critical to confirm with business stakeholders** whether this job tracking functionality is still required. If so, the specific logic for updating the `PoolVertrag` table (or its BigQuery equivalent) needs to be designed and implemented as additional BigQueryOperator tasks in the DAG.
*   **Source of Parameters:** The upstream system that calls `k_ausd_adressen.ksh` and provides parameters (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`) needs to be identified. The Airflow DAG is designed to accept these as DAG run configuration parameters. The integration with the upstream system must ensure these parameters are passed correctly when triggering the Airflow DAG.

## 6. Validation

Validation of the migrated job involves several steps to ensure functional equivalence and data integrity:

1.  **Triggering the DAG:**
    *   Manually trigger the `k_ausd_adressen_dag` in the Airflow UI.
    *   Provide the required DAG run configuration parameters:
        ```json
        {
            "p_jobkennung": "TEST_JOB",
            "p_eintragsnr": "123",
            "p_stichtag": "20231026",
            "p_wiederanlaufwert": "0",
            "gcp_conn_id": "google_cloud_default",
            "source_dataset": "your-gcp-project.your_source_dataset",
            "target_dataset": "your-gcp-project.your_target_dataset"
        }
        ```
    *   Monitor the DAG run in the Airflow UI for successful completion of all tasks.

2.  **What "Passing" Means:**
    *   **Successful DAG Run:** All tasks within the `k_ausd_adressen_dag` complete with a "success" status in the Airflow UI.
    *   **Log Verification:** Review the logs for each task, especially `parse_and_validate_parameters_task`, `calculate_dates_task`, and `log_record_count_task`, to ensure parameters are correctly parsed, dates are calculated as expected, and the record count is logged without errors.
    *   **BigQuery Job Completion:** Verify that the BigQuery job initiated by `execute_d_ausd_adressen_sql_task` completes successfully in the BigQuery console.
    *   **Record Count Verification:** Compare the record count logged by `log_record_count_task` with the expected number of records processed by the original script for a given `p_stichtag`.
    *   **Data Comparison (Critical):**
        *   Run the original `k_ausd_adressen.ksh` script for a specific `p_stichtag` in the legacy environment.
        *   Run the `k_ausd_adressen_dag` for the *same* `p_stichtag` in the new GCP environment.
        *   Extract the final output data from the relevant target tables (e.g., `sof_ta_e_reach_gp`, `sof_ta_e_business_gp`, etc.) from both the legacy Oracle database and BigQuery.
        *   Perform a row-by-row comparison of the extracted datasets to ensure exact match in content and count. Any discrepancies must be investigated and resolved.

## 7. Rollback Procedure

In case of critical issues or failure during the go-live or post-migration, the following rollback procedure can be followed:

1.  **Disable Airflow DAG:**
    *   Immediately disable the `k_ausd_adressen_dag` in the Airflow UI to prevent further execution.

2.  **Re-enable Legacy Job:**
    *   Re-enable the original `k_ausd_adressen.ksh` script in the legacy environment.
    *   Ensure its original scheduling mechanism is restored.

3.  **Data Restoration (if necessary):**
    *   If the migrated job made irreversible changes to shared target tables in BigQuery that affect other processes, or if the data in BigQuery is corrupted, restore the target tables in BigQuery to a known good state from a backup taken *before* the migration attempt.
    *   **Note:** The current `d_ausd_adressen.sql.bq` uses `TRUNCATE` before `INSERT`, which means each run effectively overwrites the target tables. If a rollback is needed, simply re-running the legacy job will populate its target tables, and the BigQuery tables will be overwritten on the next successful DAG run. However, if the BigQuery tables are considered the source for other downstream processes, a restoration might be needed.

4.  **Investigation and Remediation:**
    *   Analyze the root cause of the failure in the migrated job.
    *   Address the identified issues in the Airflow DAG, Python utilities, or BigQuery SQL.
    *   Perform thorough re-testing in a non-production environment before attempting another go-live.