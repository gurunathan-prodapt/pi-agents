# MIGRATION_NOTES.md

## 1. Summary

The KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_da_vda_tk.ksh` (version V2.0.0, "Bereitstellung Basisprodukte BERT") has been migrated. This script, originally responsible for orchestrating the extraction and provision of contract cache data for the BERT system, has been refactored and migrated to Google Cloud Platform.

The target platform consists of:
*   **Google BigQuery**: For data processing logic (BigQuery Stored Procedures) and job metadata/logging (BigQuery Tables).
*   **Google Cloud Composer (Apache Airflow)**: For scheduling and orchestration of the BigQuery components.

## 2. Generated Artifacts

The migration process generated the following artifacts:

*   **`sql/procedures/bereitstellung_basisprodukte_bert.sql`**
    *   **Role**: This BigQuery Stored Procedure serves as the main entry point and wrapper for the migrated job. It handles parameter validation, default value assignment for `Stichtag` and `Wiederanlaufwert`, initializes logging, and orchestrates the call to the core data processing logic. It also implements robust error handling and status updates.
*   **`sql/procedures/k_ausd_bp_ta_rn_da_vda_tk.sql`**
    *   **Role**: This BigQuery Stored Procedure is a stub that will encapsulate the core data extraction, transformation, and loading logic. It is designed to replace the functionality previously found in the `k_ausd_bp_ta_rn_da_vda_tk.ksh` shell script. Its full implementation is pending further analysis of the original kernel script.
*   **`sql/ddl/job_log.sql`**
    *   **Role**: Data Definition Language (DDL) for the `project.dataset.job_log` BigQuery table. This table is used for centralized logging of job execution, including informational messages, warnings, and errors, replacing the legacy file-based logging and `DWMSG` framework.
*   **`sql/ddl/job_status.sql`**
    *   **Role**: DDL for the `project.dataset.job_status` BigQuery table. This table tracks the current status (e.g., 'RUNNING', 'SUCCESS', 'FAILED') of each individual job run, providing an auditable record of job outcomes.
*   **`sql/ddl/job_control.sql`**
    *   **Role**: DDL for the `project.dataset.job_control` BigQuery table. This table stores audit information and control parameters for each job run, such as the `Stichtag`, `sysdate_ddmmyyyy`, and `Wiederanlaufwert` used.
*   **`dags/bereitstellung_basisprodukte_bert_dag.py`**
    *   **Role**: An Apache Airflow DAG (for Cloud Composer) responsible for scheduling and executing the `bereitstellung_basisprodukte_bert` BigQuery Stored Procedure. It defines how parameters like `p_stichtag` and `p_wiederanlaufWert` can be passed to the stored procedure, either as fixed values or dynamically (e.g., from Airflow's execution context).

## 3. Key Design Decisions

*   **BigQuery Stored Procedures for Script Logic**: The original KornShell script's orchestration, parameter handling, and error management logic were translated into a BigQuery Stored Procedure (`bereitstellung_basisprodukte_bert`). This centralizes the control flow within the data warehouse environment, leveraging BigQuery's native capabilities for SQL-based logic and error handling (`BEGIN...EXCEPTION WHEN ERROR THEN...END`).
*   **Two-Procedure Approach**: The decision to split the logic into two BigQuery Stored Procedures (`bereitstellung_basisprodukte_bert` as the wrapper and `k_ausd_bp_ta_rn_da_vda_tk` for core processing) mirrors the original shell script's structure (wrapper calling a kernel script). This promotes modularity, allows for independent development/testing of the core logic, and clearly separates orchestration concerns from data transformation.
*   **BigQuery Tables for Logging and Status**: The custom `DWMSG` framework and file-based logging were replaced by dedicated BigQuery tables (`job_log`, `job_status`, `job_control`). This provides a centralized, queryable, and scalable logging solution within BigQuery, enabling easier auditing, monitoring, and troubleshooting compared to distributed log files.
*   **Cloud Composer for Orchestration**: Cloud Composer (Apache Airflow) was chosen for scheduling and executing the BigQuery Stored Procedure. This provides a robust, managed, and scalable orchestration platform with features like scheduling, retries, monitoring, and parameter passing, which are essential for production data pipelines.
*   **Parameter Handling Translation**: `getopts` and shell variable assignments were replaced by BigQuery Stored Procedure input parameters and `COALESCE` for defaulting values. Date functions (`DWDate_Gib_Zeitraum`) were replaced by BigQuery's `CURRENT_DATE()` and `FORMAT_DATE()`.
*   **Error Handling Modernization**: Shell `trap` commands were replaced with BigQuery Scripting's `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks, allowing for structured error capture, logging to BigQuery tables, and re-raising errors via `SIGNAL SQLSTATE` for upstream orchestration systems.
*   **Restart Logic**: The `Wiederanlaufwert` logic, which implies conditional deletion and re-insertion, is designed to be implemented within the `k_ausd_bp_ta_rn_da_vda_tk` procedure using BigQuery's `DELETE` and `INSERT` or `MERGE` statements, filtered by `DWH_VERTRAG_ID`.

**Notable Trade-offs**:
*   **Stubbed Core Logic**: The `k_ausd_bp_ta_rn_da_vda_tk` procedure is currently a stub. This means the most complex part of the migration (the actual data transformation) is deferred, representing a significant known gap. This approach allowed for the migration of the wrapper logic and infrastructure setup to proceed, but the full benefits and validation of the migration are pending the completion of the core logic.
*   **Implicit Source/Target Tables**: The exact BigQuery table names for the "DWH contract cache" and "FOS-Tabelle" are placeholders (`project.dataset.dwh_contract_cache_table`, `project.dataset.fos_target_table`) in the stub. These will need to be explicitly defined and confirmed during the implementation of the core logic.

## 4. Manual Steps Before Go-Live

Before the migrated job can be run in production, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**:
    *   Ensure the BigQuery dataset `project.dataset` (replace `project` and `dataset` with actual project ID and dataset name) exists in your Google Cloud Project. If not, create it.
    *   `bq mk --dataset project:dataset`

2.  **Deploy BigQuery DDLs**:
    *   Execute the DDL scripts for the logging and status tables in BigQuery:
        *   `sql/ddl/job_log.sql`
        *   `sql/ddl/job_status.sql`
        *   `sql/ddl/job_control.sql`
    *   This can be done via the BigQuery UI, `bq query` command, or a deployment pipeline.

3.  **Deploy BigQuery Stored Procedures**:
    *   Deploy the `bereitstellung_basisprodukte_bert.sql` and `k_ausd_bp_ta_rn_da_vda_tk.sql` stored procedures to the `project.dataset` dataset.
    *   Note: The `k_ausd_bp_ta_rn_da_vda_tk.sql` procedure is a stub and will require full implementation based on the analysis of the original kernel script.

4.  **IAM/Permissions**:
    *   The Google Cloud service account used by Cloud Composer (or any other orchestrator) to execute BigQuery jobs must have the necessary IAM roles:
        *   `BigQuery Data Editor` (or `BigQuery Admin`) on the `project.dataset` dataset to create/update tables and execute stored procedures.
        *   `BigQuery Job User` to run BigQuery jobs.
        *   Permissions to read from source DWH tables and write to target FOS tables (once `k_ausd_bp_ta_rn_da_vda_tk` is implemented).

5.  **Cloud Composer Environment Setup**:
    *   Ensure a Cloud Composer environment is provisioned and running.
    *   Verify the `gcp_conn_id="google_cloud_default"` connection is correctly configured in Airflow to point to your Google Cloud Project.

6.  **Deploy Cloud Composer DAG**:
    *   Upload the `dags/bereitstellung_basisprodukte_bert_dag.py` file to the DAGs folder of your Cloud Composer environment.
    *   Once uploaded, the DAG should appear in the Airflow UI.

7.  **Configure Scheduling**:
    *   Update the `schedule` parameter in `dags/bereitstellung_basisprodukte_bert_dag.py` from `None` to the desired cron expression or Airflow schedule preset (e.g., `"@daily"`, `"0 0 * * *"`).

8.  **Parameter Configuration (DAG)**:
    *   Review and adjust the `stichtag_param_value` and `wiederanlaufwert_param_value` in the DAG.
    *   Decide if `p_stichtag` should default to `CURRENT_DATE()` (by setting `stichtag_param_value = None`) or be dynamically derived from Airflow's execution date (e.g., `stichtag_param_value = "{{ ds_nodash[6:] + ds_nodash[4:6] + ds_nodash[0:4] }}"`).

## 5. Known Gaps & Unresolved References

The following items were flagged during the migration design and remain as known gaps or require further follow-up:

*   **Core Logic Implementation (`k_ausd_bp_ta_rn_da_vda_tk.sql`)**: The most significant gap is the full implementation of the `k_ausd_bp_ta_rn_da_vda_tk` BigQuery Stored Procedure. This procedure currently contains only logging and placeholder comments. The actual data extraction, transformation, and loading logic, including the restart mechanism, must be translated from the original `k_ausd_bp_ta_rn_da_vda_tk.ksh` script. This requires a detailed analysis of that kernel script.
*   **Implicit Database Access Details**: The exact source tables (e.g., "DWH contract cache tables") and target tables (e.g., "FOS-Tabelle") that `k_ausd_bp_ta_rn_da_vda_tk.ksh` interacts with are not explicitly defined in the provided design. Their BigQuery equivalents (full `project.dataset.table_name`) need to be identified and confirmed for the implementation of `k_ausd_bp_ta_rn_da_vda_tk.sql`.
*   **Missing Complexity Metadata**: The absence of `file_complexity` metadata for the original script means the inherent complexity of the job was not automatically assessed. This could lead to underestimation of effort for the `k_ausd_bp_ta_rn_da_vda_tk` implementation.
*   **Date Logic Nuances**: The original script comments mention `MIN(sysdate,maxladedatum)` but the implementation defaults to `sysdate`. The precise intended behavior for `Stichtag` determination, especially concerning `maxladedatum`, needs explicit confirmation to ensure the BigQuery implementation aligns with business requirements.
*   **Unicode/Character Encoding**: The presence of special characters in the original script's comments (`ausgewhlter`) suggests potential character encoding considerations. Ensure that BigQuery handles all character sets correctly, especially if data contains non-ASCII characters.
*   **Source Data Schema**: The schema of the source DWH tables and the target FOS table, including data types and primary keys, needs to be fully understood to correctly implement the BigQuery SQL for `k_ausd_bp_ta_rn_da_vda_tk`.

## 6. Validation

Validation of the migrated job involves several steps to ensure functional correctness and operational stability.

1.  **Deploy All Artifacts**: Ensure all DDLs, Stored Procedures, and the Cloud Composer DAG are deployed to their respective environments.

2.  **Manual Stored Procedure Execution (Initial Test)**:
    *   Before relying on the DAG, manually execute the main BigQuery Stored Procedure `project.dataset.bereitstellung_basisprodukte_bert` from the BigQuery console.
    *   **Test Cases**:
        *   Call with `p_stichtag = NULL` and `p_wiederanlaufWert = NULL` (to test defaults).
        *   Call with a specific valid `p_stichtag` (e.g., `'31122023'`) and `p_wiederanlaufWert = 0`.
        *   Call with a specific valid `p_stichtag` and a non-zero `p_wiederanlaufWert` (to test restart logic once `k_ausd_bp_ta_rn_da_vda_tk` is implemented).
        *   Call with an invalid `p_stichtag` format (e.g., `'2023-12-31'`) to verify error handling.

3.  **Cloud Composer DAG Trigger**:
    *   Trigger the `bereitstellung_basisprodukte_bert_dag` from the Airflow UI.
    *   Monitor the DAG run in the Airflow UI for successful completion of tasks.

4.  **Logging and Status Verification**:
    *   **`project.dataset.job_log`**: Query this table to verify that:
        *   `INFO` messages are recorded for job start, parameter processing, and completion.
        *   No `ERROR` messages are present for successful runs.
        *   Error details are correctly logged for failed runs.
    *   **`project.dataset.job_status`**: Query this table to verify that:
        *   A record exists for the job run with the correct `job_kennung` and `job_entry_nr`.
        *   The `status` field transitions from 'RUNNING' to 'SUCCESS' (or 'FAILED' on error).
    *   **`project.dataset.job_control`**: Query this table to verify that:
        *   The `stichtag`, `sysdate_ddmmyyyy`, and `restart_value` parameters used for the run are correctly recorded.

5.  **Data Output Verification (Post `k_ausd_bp_ta_rn_da_vda_tk` Implementation)**:
    *   Once `k_ausd_bp_ta_rn_da_vda_tk` is fully implemented, query the target FOS table to confirm:
        *   Data is loaded correctly based on the `Stichtag`.
        *   The restart logic (deletion/re-insertion based on `Wiederanlaufwert`) functions as expected.
        *   Data transformations are applied accurately.
        *   Record counts and key metrics match expectations from the source system or previous runs.

**"Passing" Criteria**:
*   The `bereitstellung_basisprodukte_bert_dag` completes successfully in Cloud Composer.
*   The `project.dataset.job_status` table shows a `SUCCESS` status for the corresponding job run.
*   The `project.dataset.job_log` table contains a final `INFO` message indicating "Job completed successfully."
*   (Crucially, after `k_ausd_bp_ta_rn_da_vda_tk` implementation) The target FOS table contains the expected, correctly transformed data, adhering to the `Stichtag` and `Wiederanlaufwert` logic.

## 7. Rollback Procedure

In case of critical issues or unexpected behavior after go-live, the following rollback procedure can be executed:

1.  **Pause/Unschedule Cloud Composer DAG**:
    *   In the Airflow UI, pause the `bereitstellung_basisprodukte_bert_dag` to prevent further executions.

2.  **Delete Cloud Composer DAG**:
    *   Remove the `dags/bereitstellung_basisprodukte_bert_dag.py` file from the Cloud Composer DAGs folder.

3.  **Delete BigQuery Stored Procedures**:
    *   Drop the migrated BigQuery Stored Procedures from the `project.dataset` dataset:
        *   `DROP PROCEDURE IF EXISTS project.dataset.bereitstellung_basisprodukte_bert;`
        *   `DROP PROCEDURE IF EXISTS project.dataset.k_ausd_bp_ta_rn_da_vda_tk;`

4.  **Revert Target Data (if necessary)**:
    *   If the `k_ausd_bp_ta_rn_da_vda_tk` procedure had already written data to the target FOS table, and this data is incorrect or corrupted, a data rollback might be necessary. This could involve:
        *   Restoring the target table from a previous backup.
        *   Deleting the affected data based on the `Stichtag` and `job_entry_nr` (if tracked in the target table).
        *   **Note**: This step is highly dependent on the specific implementation of `k_ausd_bp_ta_rn_da_vda_tk` and the target table's schema/partitioning.

5.  **Retain Logging/Status Tables (Recommended)**:
    *   It is generally recommended to *not* delete the `job_log`, `job_status`, and `job_control` tables during a rollback, as they contain valuable audit information about the failed migration attempt. If deletion is absolutely necessary, ensure logs are archived first.

6.  **Revert to Original Execution**:
    *   Ensure the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_da_vda_tk.ksh` script and its associated kernel script are re-enabled and scheduled in their legacy environment.