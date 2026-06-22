# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell wrapper script `r_ausd_v_ta_p_discount_rr.ksh` from a legacy Unix environment to Google Cloud Platform (GCP). The original script orchestrated a data reconciliation process, handling environment setup, parameter parsing, logging, error trapping, and invoking a core processing script (`k_ausd_v_ta_p_discount_rr.ksh`).

The migration re-platforms this orchestration logic to BigQuery and Cloud Composer (Airflow). The shell script's wrapper functionality has been translated into a BigQuery Stored Procedure (`vertragsdatenabgleich`), which will then call a separate BigQuery Stored Procedure representing the core reconciliation logic (`core_discount_rr_process`). Cloud Composer (Airflow) is used for scheduling and overall workflow management. File-based logging is replaced by a dedicated BigQuery audit log table.

## 2. Generated artifacts

The migration produced the following artifacts:

*   **`sql/ddl/job_audit_log.sql`**
    *   **Role:** Defines the Data Definition Language (DDL) for the `job_audit_log` table in BigQuery. This table serves as the central repository for all job execution metadata, status, errors, and logging information, replacing the original shell script's file-based logging and `DWMSG_*` functions. It includes fields for `job_id`, `status`, `error_code`, `message`, and `parameters`, among others.

*   **`sql/stored_procedures/vertragsdatenabgleich.sql`**
    *   **Role:** This BigQuery Stored Procedure (`my_gcp_project.my_bq_dataset.vertragsdatenabgleich`) is the direct replacement for the `r_ausd_v_ta_p_discount_rr.ksh` wrapper script. It encapsulates the logic for:
        *   Parsing input parameters (`p_stichtag`, `p_log_level`).
        *   Initializing job metadata and logging job start/end events to `job_audit_log`.
        *   Implementing parameter validation.
        *   Orchestrating the call to the core reconciliation logic (`core_discount_rr_process`).
        *   Handling errors using BigQuery's `BEGIN...EXCEPTION` blocks and logging failures to `job_audit_log`.

*   **`sql/stored_procedures/core_discount_rr_process.sql`**
    *   **Role:** This BigQuery Stored Procedure (`my_gcp_project.my_bq_dataset.core_discount_rr_process`) is a placeholder for the actual data reconciliation logic originally contained within `k_ausd_v_ta_p_discount_rr.ksh`. It demonstrates how parameters are passed to the core logic and includes comments indicating where the translated SQL for the reconciliation process would reside. Its full implementation requires a separate analysis of the original `k_ausd_v_ta_p_discount_rr.ksh` script.

*   **`dags/r_ausd_v_ta_p_discount_rr_dag.py`**
    *   **Role:** An Apache Airflow Directed Acyclic Graph (DAG) written in Python. This DAG is responsible for scheduling and orchestrating the execution of the `vertragsdatenabgleich` BigQuery Stored Procedure. It defines the `start_date`, `schedule`, and uses the `BigQueryExecuteStoredProcedureOperator` to invoke the migrated wrapper logic, passing the necessary parameters.

## 3. Key design decisions

*   **Wrapper Logic to BigQuery Stored Procedure:** The orchestration logic of the original KornShell wrapper (`r_ausd_v_ta_p_discount_rr.ksh`) was directly translated into a BigQuery Stored Procedure (`vertragsdatenabgleich`). This decision leverages BigQuery's native capabilities for procedural logic, parameter handling, and error management, keeping the core processing close to the data.
*   **Centralized Audit Logging in BigQuery:** File-based logging and custom `DWMSG_*` functions were replaced by a dedicated `job_audit_log` table in BigQuery. This provides a structured, queryable, and centralized logging mechanism, improving observability and simplifying auditing compared to distributed log files.
*   **BigQuery Error Handling:** The shell script's `trap` mechanism for error handling was replaced by BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks within the stored procedures. This provides robust, SQL-native error management and allows for detailed error logging to the `job_audit_log` table.
*   **Cloud Composer (Airflow) for Orchestration:** Airflow was chosen to replace the shell script's role as the primary orchestrator. This provides a managed, scalable, and feature-rich platform for scheduling, monitoring, and managing complex data workflows, integrating seamlessly with other GCP services.
*   **Modular BigQuery Stored Procedures:** The wrapper logic and the core reconciliation logic are separated into two distinct BigQuery Stored Procedures (`vertragsdatenabgleich` and `core_discount_rr_process`). This promotes modularity, reusability, and clearer separation of concerns, making future maintenance and independent development easier.
*   **Parameter Mapping:** Command-line parameters (`-s`, `-l`) from the original shell script are directly mapped to input arguments (`p_stichtag`, `p_log_level`) of the BigQuery Stored Procedures. This maintains functional parity and clarity.
*   **Trade-offs:**
    *   **Loss of direct OS interaction:** The migration moves away from direct operating system commands and file system interactions, requiring re-implementation of such functionalities using BigQuery SQL or Python within Airflow.
    *   **BigQuery SQL expertise:** The solution requires proficiency in BigQuery SQL for developing and maintaining the stored procedures.
    *   **Dependency on GCP services:** The migrated solution is tightly coupled with BigQuery and Cloud Composer, introducing dependencies on GCP infrastructure and services.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:**
    *   Ensure the BigQuery dataset `my_bq_dataset` exists within `my_gcp_project`. If not, create it.
    ```bash
    bq mk --project_id my_gcp_project my_bq_dataset
    ```

2.  **Deploy `job_audit_log` Table:**
    *   Execute the DDL from `sql/ddl/job_audit_log.sql` to create the audit log table in BigQuery.
    ```bash
    bq query --use_legacy_sql=false --project_id my_gcp_project < sql/ddl/job_audit_log.sql
    ```

3.  **Deploy BigQuery Stored Procedures:**
    *   Execute the DDL for `vertragsdatenabgleich.sql` and `core_discount_rr_process.sql` to create or replace these stored procedures in BigQuery.
    ```bash
    bq query --use_legacy_sql=false --project_id my_gcp_project < sql/stored_procedures/vertragsdatenabgleich.sql
    bq query --use_legacy_sql=false --project_id my_gcp_project < sql/stored_procedures/core_discount_rr_process.sql
    ```

4.  **Migrate Core Reconciliation Logic:**
    *   **Crucially**, the `sql/stored_procedures/core_discount_rr_process.sql` is currently a placeholder. The actual SQL logic from the original `k_ausd_v_ta_p_discount_rr.ksh` script must be translated and implemented within this stored procedure. This may involve creating additional BigQuery tables (e.g., `ta_p_discount_rr` if it's a target table) or views.

5.  **IAM/Permissions:**
    *   The service account associated with your Cloud Composer environment (or the user running the DAG) must have sufficient IAM permissions:
        *   `BigQuery Data Editor` or `BigQuery User` on `my_gcp_project.my_bq_dataset` to create/update tables and execute stored procedures.
        *   `BigQuery Job User` to run BigQuery jobs.
        *   Appropriate permissions for any other GCP resources accessed by the core logic.

6.  **Airflow Connection:**
    *   Ensure the `google_cloud_default` connection is configured correctly in your Airflow environment. This connection is typically pre-configured in Cloud Composer.

7.  **Airflow DAG Deployment:**
    *   Upload the `dags/r_ausd_v_ta_p_discount_rr_dag.py` file to the DAGs folder of your Cloud Composer environment.

8.  **Airflow DAG Scheduling:**
    *   Review and configure the `schedule` parameter within the `r_ausd_v_ta_p_discount_rr_dag.py` to match the desired execution frequency. The current example has `schedule=None`.
    *   Adjust the `p_stichtag_val` and `p_log_level_val` in the DAG to use Airflow macros (e.g., `{{ ds_nodash }}`) for dynamic date generation, if required, instead of hardcoded values.

## 5. Known gaps & unresolved references

The following items were identified as gaps or require further follow-up:

*   **Core Logic Unknown:** The most significant unresolved item is the actual data reconciliation logic within `k_ausd_v_ta_p_discount_rr.ksh`. Its complexity and specific operations will dictate the strategy for its BigQuery migration (e.g., pure SQL, UDFs, Python scripts). The `core_discount_rr_process` stored procedure is a placeholder and needs full implementation.
*   **Parameter Usage Clarification:** The original shell script parsed `-s` and `-l` parameters but did not explicitly use them within the wrapper. While they are passed to the BigQuery Stored Procedures, their exact intended use within the *core* reconciliation logic needs to be confirmed.
*   **Environment Variables:** The resolution of `BERT_DIR_ROOT` and other environment variables set by `.dw_init` needs to be fully mapped to a BigQuery-compatible configuration mechanism or Airflow variables/connections.
*   **Timestamp Format:** The original `date +%d%m%Y` format for `v_sysdate` needs to be consistently handled if `p_stichtag` is expected in this format. The current BigQuery SP expects `YYYYMMDD`.
*   **Error Numbering:** The meaning of `ErrNr=193` and `ErrNr=192` from the original script should be documented or mapped to specific BigQuery error codes or custom error handling conventions for consistency.
*   **`tee -a $LogDatei` behavior:** Replicating the exact concurrent logging behavior of `tee -a` (appending to a file from multiple sources) with BigQuery audit tables should be carefully considered, especially if the core script had complex, concurrent logging patterns. The current BigQuery audit log is designed for sequential entries.
*   **`ta_p_discount_rr` Table DDL:** If the core reconciliation logic (`k_ausd_v_ta_p_discount_rr.ksh`) directly manipulates or creates the `ta_p_discount_rr` table, its DDL must be defined and created in BigQuery.

## 6. Validation

To validate the migrated job, follow these steps:

1.  **Unit Test BigQuery Stored Procedures:**
    *   **`job_audit_log`:** Verify that `INSERT` statements correctly populate the `job_audit_log` table with expected fields and values.
    *   **`vertragsdatenabgleich`:**
        *   Execute the stored procedure directly in BigQuery with valid and invalid `p_stichtag` and `p_log_level` parameters.
        *   Verify that successful runs log `status = 'SUCCESS'` and failed runs log `status = 'FAILED'` with appropriate `error_code` and `error_message` in `job_audit_log`.
        *   Ensure parameter validation (e.g., for `p_stichtag` format) works as expected.
    *   **`core_discount_rr_process`:**
        *   Once the core logic is implemented, execute it directly with various inputs.
        *   Verify that it performs the expected data transformations and updates the `ta_p_discount_rr` table (or other target tables) correctly.

2.  **Integration Test Airflow DAG:**
    *   Trigger the `r_ausd_v_ta_p_discount_rr_dag` manually in the Airflow UI.
    *   Monitor the DAG run in the Airflow UI for successful task completion.
    *   Check the Airflow task logs for any errors or unexpected output from the `BigQueryExecuteStoredProcedureOperator`.

3.  **"Passing" Criteria:**
    *   The `execute_vertragsdatenabgleich_sp` task in the Airflow DAG completes successfully (green status in Airflow UI).
    *   The `job_audit_log` table contains a complete record for the job run, with `status = 'SUCCESS'`, and accurate `start_timestamp`, `end_timestamp`, and `message` entries.
    *   (Once `core_discount_rr_process` is implemented) The `ta_p_discount_rr` table (or other target tables) reflects the expected data changes and reconciliation results as per the original script's functionality.
    *   No unexpected errors are reported in BigQuery job logs or Airflow task logs.

## 7. Rollback procedure

In case of issues or unexpected behavior after go-live, the following rollback procedure can be followed:

1.  **Disable Airflow DAG:**
    *   In the Airflow UI, pause or delete the `r_ausd_v_ta_p_discount_rr_dag` to prevent further executions of the migrated job.

2.  **Revert to Original Script:**
    *   Re-enable the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_p_discount_rr.ksh` job in its legacy scheduling system.

3.  **BigQuery Artifacts (Optional):**
    *   If the BigQuery stored procedures or the `job_audit_log` table are not used by other processes, they can be dropped.
        ```sql
        DROP PROCEDURE IF EXISTS `my_gcp_project.my_bq_dataset.vertragsdatenabgleich`;
        DROP PROCEDURE IF EXISTS `my_gcp_project.my_bq_dataset.core_discount_rr_process`;
        -- Only drop if no other jobs rely on it
        -- DROP TABLE IF EXISTS `my_gcp_project.my_bq_dataset.job_audit_log`;
        ```
    *   If the `ta_p_discount_rr` table was modified or created by the migrated core logic, revert any changes or drop the table if it was newly created and not needed.

4.  **Monitor Original Job:**
    *   Verify that the original KornShell script executes successfully and produces the expected output in the legacy environment.