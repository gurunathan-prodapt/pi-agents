# MIGRATION_NOTES.md

## 1. Summary

The KornShell wrapper script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_action_assoc.ksh` has been migrated to Google Cloud Platform. This script, responsible for orchestrating a contract data reconciliation process for the `ta_action_assoc` table, has been transformed into a BigQuery Stored Procedure, with its execution managed by an Apache Airflow DAG on Google Cloud Composer.

The migration involved:
*   Re-implementing shell-based orchestration logic (parameter parsing, logging, error handling, and invocation of a core script) into a BigQuery Stored Procedure.
*   Replacing file-based logging with dedicated BigQuery logging tables.
*   Creating a placeholder BigQuery Stored Procedure for the core business logic (`k_ausd_v_ta_action_assoc.ksh`), which is a separate migration effort.
*   Establishing an Airflow DAG to schedule and trigger the BigQuery Stored Procedure.

## 2. Generated artifacts

The migration process generated the following files:

*   **`ddl/job_log_table.sql`**
    *   **Role:** BigQuery Data Definition Language (DDL) script to create the `job_log` table. This table centralizes all job execution metadata, including start/end times, status, and job-specific identifiers, replacing the file-based logging of the original script.
*   **`ddl/job_error_log_table.sql`**
    *   **Role:** BigQuery DDL script to create the `job_error_log` table. This table stores detailed error information, including error codes, arguments, and messages, providing a structured and queryable error history. It replaces the error logging functionality of the original script.
*   **`sp/k_ausd_v_ta_action_assoc.sql`**
    *   **Role:** A placeholder BigQuery Stored Procedure. This procedure represents the future migration target for the core business logic originally contained in `k_ausd_v_ta_action_assoc.ksh`. Currently, it only logs its invocation, serving as a dependency for the wrapper procedure. The actual data transformation logic for `ta_action_assoc` will be implemented here in a subsequent migration phase.
*   **`sp/vertragsdatenabgleich.sql`**
    *   **Role:** The primary migrated component, a BigQuery Stored Procedure that replaces the original `r_ausd_v_ta_action_assoc.ksh` wrapper script. It handles parameter validation, job metadata initialization, logging to BigQuery tables, error handling, and the invocation of the `k_ausd_v_ta_action_assoc` stored procedure.
*   **`orchestration/dag_vertragsdatenabgleich.py`**
    *   **Role:** An Apache Airflow DAG written in Python. This DAG is responsible for orchestrating the execution of the `vertragsdatenabgleich` BigQuery Stored Procedure. It provides scheduling, parameter passing, and monitoring capabilities, replacing the manual or cron-based execution of the original shell script.

## 3. Key design decisions

*   **Migration of Wrapper Logic to BigQuery Stored Procedure**: The orchestration and control flow logic of the original KornShell script was re-implemented as a BigQuery Stored Procedure (`vertragsdatenabgleich`). This decision was made to:
    *   **Keep logic close to data**: BigQuery Stored Procedures execute within the BigQuery environment, minimizing data movement and leveraging BigQuery's performance.
    *   **Leverage BigQuery Scripting**: BigQuery's SQL scripting capabilities provide robust control flow, variable management, and error handling, effectively replacing shell script constructs.
    *   **Standardization**: Aligns with a cloud-native, SQL-centric approach for data warehousing operations.
    *   **Observability**: BigQuery provides logging and monitoring for stored procedure executions.
*   **Centralized Logging to BigQuery Tables**: Instead of file-based logging, all job and error logs are now written to dedicated BigQuery tables (`job_log`, `job_error_log`). This offers:
    *   **Queryability**: Logs can be easily queried and analyzed using SQL.
    *   **Scalability**: BigQuery handles large volumes of log data efficiently.
    *   **Centralization**: Provides a single source of truth for job execution history across multiple migrated jobs.
    *   **Integration**: Easier integration with other GCP monitoring and alerting services.
*   **Placeholder for Core Business Logic (`k_ausd_v_ta_action_assoc`)**: A deliberate decision was made to create a placeholder stored procedure for the core kernel script. This allows for a phased migration, addressing the wrapper first and deferring the potentially more complex data transformation logic to a separate, dedicated migration effort. This isolates the complexity and allows for independent testing of the orchestration layer.
*   **Orchestration with Apache Airflow (Cloud Composer)**: The scheduling and execution management of the BigQuery Stored Procedure are handled by an Airflow DAG. This provides:
    *   **Managed Service**: Cloud Composer offers a fully managed Airflow environment.
    *   **Robust Scheduling**: Advanced scheduling options and dependency management.
    *   **Monitoring & Alerting**: Centralized monitoring, logging, and alerting capabilities.
    *   **Idempotency & Retries**: Built-in mechanisms for handling failures and retries.
    *   **Parameterization**: Easy passing of dynamic parameters to the BigQuery Stored Procedure.
*   **Parameter Handling Transformation**: The `getopts` mechanism from KornShell is replaced by `IN` parameters in the BigQuery Stored Procedure, providing a clear and type-safe interface.
*   **Error Handling Transformation**: Shell `trap` commands are replaced by BigQuery's `BEGIN ... EXCEPTION WHEN ERROR THEN ... END;` blocks and `SIGNAL SQLSTATE` for structured error management.

**Notable Trade-offs**:
*   **Increased Complexity for Simple Wrappers**: For very simple shell scripts, migrating to BigQuery Stored Procedures and Airflow can introduce more overhead than the original script. However, this is justified by the benefits of scalability, observability, and standardization in a cloud environment.
*   **Dependency on Core Logic Migration**: The full functionality of the job is dependent on the successful migration of `k_ausd_v_ta_action_assoc.ksh`, which is currently a placeholder.
*   **Loss of Direct Filesystem Access**: The ability to write arbitrary files for logging or temporary data is replaced by structured logging to BigQuery tables, which requires a different approach for debugging and data inspection.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**:
    *   Ensure the target BigQuery dataset (`your_project.your_dataset`) exists. If not, create it:
        ```bash
        bq mk --dataset --default_table_expiration 365 `your_project`:`your_dataset`
        ```
    *   **Action**: Replace `your_project` and `your_dataset` with actual project and dataset IDs.

2.  **IAM Permissions Configuration**:
    *   **BigQuery Service Account**: Ensure the service account used for BigQuery operations (e.g., by Cloud Composer) has the necessary roles:
        *   `BigQuery Data Editor` (to write to `job_log` and `job_error_log` and execute stored procedures).
        *   `BigQuery Job User` (to run BigQuery jobs).
        *   `BigQuery Data Viewer` (to read from any necessary source tables).
    *   **Cloud Composer Service Account**: The service account associated with your Cloud Composer environment needs permissions to interact with BigQuery. Typically, `Composer Worker` and `BigQuery Job User` roles are sufficient, along with `BigQuery Data Editor` for the specific dataset.
    *   **Action**: Grant required IAM roles to the relevant service accounts.

3.  **Deploy BigQuery DDLs**:
    *   Execute the DDL scripts to create the logging tables:
        ```bash
        bq query --use_legacy_sql=false < ddl/job_log_table.sql
        bq query --use_legacy_sql=false < ddl/job_error_log_table.sql
        ```
    *   **Action**: Ensure `your_project` and `your_dataset` placeholders are updated in the DDL files before execution.

4.  **Deploy BigQuery Stored Procedures**:
    *   Deploy the placeholder core procedure and the wrapper procedure:
        ```bash
        bq query --use_legacy_sql=false < sp/k_ausd_v_ta_action_assoc.sql
        bq query --use_legacy_sql=false < sp/vertragsdatenabgleich.sql
        ```
    *   **Action**: Ensure `your_project` and `your_dataset` placeholders are updated in the SP files before deployment.

5.  **Airflow Connection Configuration**:
    *   Verify that the `google_cloud_default` connection exists and is correctly configured in your Airflow environment (Cloud Composer). This connection is used by the `BigQueryStoredProcedureOperator`.
    *   **Action**: Check Airflow UI -> Admin -> Connections.

6.  **Deploy Airflow DAG**:
    *   Upload the `orchestration/dag_vertragsdatenabgleich.py` file to the DAGs folder of your Cloud Composer environment.
    *   **Action**: Ensure `BIGQUERY_PROJECT_ID` and `BIGQUERY_DATASET_ID` are updated in the DAG file.
    *   **Scheduling**: If the DAG is intended to run on a schedule, ensure `schedule` is set appropriately in the DAG definition (e.g., `schedule="@daily"`). If it's event-driven, `schedule=None` is correct.

## 5. Known gaps & unresolved references

*   **Core Kernel Script (`k_ausd_v_ta_action_assoc.ksh`) Migration**: This is the most significant outstanding item. The current migration only provides a placeholder BigQuery Stored Procedure (`sp/k_ausd_v_ta_action_assoc.sql`). The actual business logic for `ta_action_assoc` reconciliation needs a separate, detailed migration design and implementation. The full functionality of the job will not be realized until this core component is migrated.
*   **`DWMSG` Framework Fidelity**: The original script relied on a custom `DWMSG_*` framework for logging and error handling. While BigQuery tables replace the storage mechanism, the exact replication of custom error codes, specific log message formats, and any advanced features of the `DWMSG` framework needs thorough validation.
*   **Environment Variables and Utility Scripts**: The original script sourced `.dw_init` and other utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`). While their core functionalities (logging, parameter parsing, date formatting) are re-implemented in the BigQuery Stored Procedure, any specific environment variables or complex utility functions not directly translated might represent a gap.
*   **Error Handling Robustness**: Replicating the exact behavior of shell `trap INT` and `trap ERR` in all edge cases within BigQuery's `EXCEPTION WHEN ERROR` blocks requires careful testing.
*   **Missing `file_complexity` data**: As noted in the design document, the absence of complexity data for the original script meant that some migration challenges might not have been fully anticipated.

## 6. Validation

To validate the migrated job, follow these steps:

1.  **Manual BigQuery Stored Procedure Execution**:
    *   Open the BigQuery console.
    *   Execute the `vertragsdatenabgleich` stored procedure directly:
        ```sql
        CALL `your_project.your_dataset.vertragsdatenabgleich`(
          p_jobkennung => 'MANUAL_TEST_JOB',
          p_run_date => CURRENT_DATE(),
          p_enable_help => FALSE
        );
        ```
    *   **Test Help Functionality**:
        ```sql
        CALL `your_project.your_dataset.vertragsdatenabgleich`(
          p_jobkennung => 'HELP_TEST',
          p_run_date => CURRENT_DATE(),
          p_enable_help => TRUE
        );
        ```
        *Expected*: A result set showing program name, version, call syntax, and description.
    *   **Test Parameter Validation**:
        ```sql
        CALL `your_project.your_dataset.vertragsdatenabgleich`(
          p_jobkennung => NULL, -- This should trigger the ErrNr 193
          p_run_date => CURRENT_DATE(),
          p_enable_help => FALSE
        );
        ```
        *Expected*: An error message `Parameterfehler: 193 p_jobkennung` and an entry in `job_error_log`.

2.  **Airflow DAG Execution**:
    *   Access the Airflow UI in Cloud Composer.
    *   Unpause the `vertragsdatenabgleich_workflow` DAG.
    *   Manually trigger the DAG.
    *   Monitor the DAG run in the Airflow UI for successful completion.

**What "passing" means**:

*   **Successful Execution**: The `vertragsdatenabgleich` stored procedure (whether called manually or via Airflow) completes without unhandled errors.
*   **Log Entries**:
    *   For successful runs, verify that entries are created in `your_project.your_dataset.job_log` with `status = 'STARTED'` and `status = 'OK'`.
    *   For error scenarios (e.g., parameter validation), verify entries in `your_project.your_dataset.job_error_log` and a corresponding `status = 'ERROR'` in `job_log`.
*   **Core Procedure Invocation**: The `k_ausd_v_ta_action_assoc` placeholder procedure should log its invocation in the `job_log` table (status 'INVOKED').
*   **Output Messages**: The `SELECT 'Die Abarbeitung wurde ohne erkennbare Fehler beendet' AS message;` should be visible in the BigQuery job results for successful runs.
*   **Parameter Handling**: All input parameters are correctly processed and reflected in the logs or behavior.

## 7. Rollback procedure

In case of critical issues or unexpected behavior after go-live, the following rollback procedure can be initiated:

1.  **Immediate Action - Stop New Executions**:
    *   **Airflow**: Pause the `vertragsdatenabgleich_workflow` DAG in the Airflow UI to prevent any further scheduled or manual runs of the migrated job.
    *   **BigQuery**: Communicate to users/systems to cease direct calls to `your_project.your_dataset.vertragsdatenabgleich` stored procedure.

2.  **Revert to Legacy System**:
    *   Re-enable the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_action_assoc.ksh` script in its legacy execution environment (e.g., cron job).
    *   Ensure all necessary environment variables, dependencies, and configurations for the legacy script are restored and functional.
    *   Verify that the legacy job can execute successfully and process data as expected.

3.  **Monitor Legacy System**:
    *   Closely monitor the re-enabled legacy job for a defined period to ensure stability and correct operation.

4.  **Cleanup (Optional, post-rollback confirmation)**:
    *   Once the legacy system is confirmed stable, the migrated artifacts can be removed from Google Cloud Platform if desired, or kept for post-mortem analysis and re-migration efforts.
        *   Delete the Airflow DAG from the Composer environment.
        *   Drop the BigQuery Stored Procedures:
            ```sql
            DROP PROCEDURE IF EXISTS `your_project.your_dataset.vertragsdatenabgleich`;
            DROP PROCEDURE IF EXISTS `your_project.your_dataset.k_ausd_v_ta_action_assoc`;
            ```
        *   Drop the BigQuery logging tables (use caution as this deletes historical log data):
            ```sql
            DROP TABLE IF EXISTS `your_project.your_dataset.job_log`;
            DROP TABLE IF EXISTS `your_project.your_dataset.job_error_log`;
            ```