# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the `r_aurd_rechstan.ksh` job, originally a KornShell script orchestrating data extraction for invoice-related contract data, to Google BigQuery. The legacy job, responsible for generating a snapshot for demand scoring based on a specified cutoff date, has been re-architected into BigQuery stored procedures and supporting BigQuery tables.

The migration targets Google BigQuery for all data processing and orchestration logic, replacing the KornShell scripts and their embedded SQL execution.

## 2. Generated artifacts

The following BigQuery artifacts have been generated as part of this migration:

*   **`bigquery/ddl/job_log.sql`**
    *   **Role:** Defines the `project.dataset.job_log` table. This table serves as a centralized audit log for all job executions, capturing timestamps, log levels, messages, and job-specific parameters like `stichtag` and `wiederanlauf_wert`. It replaces the file-based logging mechanism of the original KornShell script.
*   **`bigquery/ddl/job_status.sql`**
    *   **Role:** Defines the `project.dataset.job_status` table. This table tracks the overall status of the job, including its last run timestamp, current status (e.g., OK, FAILED, RUNNING), and parameters used in the last run. It replaces the implicit status tracking and `DWMSG_SetzeStatusOK` calls from the legacy system.
*   **`bigquery/stored_procedures/k_aurd_rechstan.sql`**
    *   **Role:** Defines the `project.dataset.k_aurd_rechstan` BigQuery stored procedure. This procedure encapsulates the core data extraction and transformation logic, directly replacing the `k_aurd_rechstan.ksh` script and its execution of `D_AURD_RECHSTAN.SQL`. It handles the restart logic (deleting and re-inserting data based on `dwh_vertrag_id`) and the primary data selection from the source to the target table.
*   **`bigquery/stored_procedures/erzeugung_abzug_rechnungsdaten.sql`**
    *   **Role:** Defines the `project.dataset.erzeugung_abzug_rechnungsdaten` BigQuery stored procedure. This is the main orchestrating procedure, replacing the `r_aurd_rechstan.ksh` wrapper script. It handles parameter parsing, `Stichtag` (cutoff date) determination (including fallback logic), logging job start/end, updating job status, and invoking the core processing procedure (`k_aurd_rechstan`).

## 3. Key design decisions

*   **BigQuery Stored Procedures for Orchestration and Logic:** The entire KornShell-based orchestration (`r_aurd_rechstan.ksh`) and core processing (`k_aurd_rechstan.ksh` and `D_AURD_RECHSTAN.SQL`) have been consolidated into two interconnected BigQuery stored procedures. This decision leverages BigQuery's native capabilities for procedural logic, parameter handling, and direct SQL execution, eliminating the need for external shell scripts and their associated runtime environments.
*   **Dedicated BigQuery Tables for Logging and Status:** Instead of file-based logging and implicit status management, two dedicated BigQuery tables (`job_log` and `job_status`) were created. This provides a structured, queryable, and centralized mechanism for auditing job executions, tracking status, and facilitating troubleshooting, aligning with modern data warehousing practices.
*   **Direct Translation of Logic to BigQuery SQL:** The transformation logic, including parameter parsing, date calculations, restart mechanisms, and data selection filters, has been directly translated into BigQuery SQL functions and procedural statements. This minimizes the introduction of new logic and aims for functional equivalence with the original job.
*   **Handling of External Dependencies:** Legacy shell script dependencies (e.g., `.dw_init`, helper scripts like `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) have been absorbed into the BigQuery stored procedure logic. This means BigQuery's built-in functions, control flow, and the new logging/status tables replace the functionality provided by these external scripts, simplifying the deployment and execution environment.
*   **Error Handling via `EXCEPTION WHEN ERROR`:** BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN...END;` blocks are used for robust error handling within the stored procedures, replacing the `trap` commands in the KornShell script. This ensures that errors are caught, logged, and propagated, allowing for proper monitoring and alerting.
*   **Optional Cloud Composer Integration:** While not part of the core migration, the design acknowledges Cloud Composer (Airflow) as an optional but recommended orchestrator. This decision provides a path for robust scheduling, dependency management, and monitoring capabilities that surpass simple cron-based scheduling of the original ksh script.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:**
    *   Ensure the target BigQuery dataset (`project.dataset`) exists. If not, create it:
        ```bash
        bq mk --dataset project:dataset
        ```
2.  **Schema/Table Creation:**
    *   Execute the DDL scripts to create the necessary logging and status tables:
        ```bash
        bq query --use_legacy_sql=false < bigquery/ddl/job_log.sql
        bq query --use_legacy_sql=false < bigquery/ddl/job_status.sql
        ```
    *   **Target Table (`project.dataset.target_table`):** Ensure the target table for the extracted invoice data exists and has the correct schema. This table's DDL is *not* part of the generated artifacts and must be created manually based on the expected output of the `D_AURD_RECHSTAN.SQL` equivalent.
    *   **Source Table (`project.dataset.source_table`):** Ensure the source table(s) corresponding to the DWH contract cache tables exist in BigQuery and are populated with the necessary data. This table's DDL is *not* part of the generated artifacts.
3.  **IAM/Permissions:**
    *   Grant the service account that will execute the BigQuery stored procedures the necessary IAM roles:
        *   `BigQuery Data Editor` on `project.dataset` to allow `INSERT`, `UPDATE`, `DELETE` operations on `job_log`, `job_status`, and `target_table`.
        *   `BigQuery Data Viewer` on `project.dataset` (or specific source datasets) to allow `SELECT` operations on `source_table`.
        *   `BigQuery Job User` to allow execution of BigQuery jobs (including stored procedures).
4.  **Connection Strings/Secrets:**
    *   BigQuery stored procedures do not require explicit connection strings in the same way a shell script connecting to an external database would. Authentication is handled via the service account executing the procedures.
    *   No specific secrets are directly managed by the stored procedures themselves. If the `source_table` or `target_table` are in different projects or require specific access, ensure the executing service account has cross-project permissions.
5.  **Stored Procedure Deployment:**
    *   Deploy the generated BigQuery stored procedures:
        ```bash
        bq query --use_legacy_sql=false < bigquery/stored_procedures/k_aurd_rechstan.sql
        bq query --use_legacy_sql=false < bigquery/stored_procedures/erzeugung_abzug_rechnungsdaten.sql
        ```
6.  **Scheduling (if using Cloud Composer/Airflow):**
    *   If Cloud Composer is used, deploy the corresponding Airflow DAG (e.g., `airflow/dags/aurd_rechstan_dag.py`) to the Composer environment. Configure the DAG to call the `erzeugung_abzug_rechnungsdaten` stored procedure with appropriate parameters and scheduling.

## 5. Known gaps & unresolved references

*   **Missing Complexity Data:** The original `file_complexity` analysis for `r_aurd_rechstan.ksh` was unavailable. This means the migration proceeded with an assumed medium complexity, and any hidden complexities within the original script's logic might not have been fully captured or translated.
*   **Commented-out `p_stichtag` Fallback:** The original script had commented-out logic for deriving `p_stichtag` from `FOSHoleLadedatum "DWH\\$TA_C_VERTRAG" v_ladedatum`. The migrated code implements a fallback using `LEAST(CURRENT_DATE(), MAX(ladedatum))` from `project.dataset.source_table`. It is crucial to confirm if this fallback logic accurately reflects the business requirement of the commented-out section.
*   **Shell Traps:** The `trap` commands in the KornShell script (for `INT`, `STOP`, `CONT`, `ERR`) handle process signals. BigQuery stored procedures do not have a direct equivalent. Error handling relies on BigQuery's `EXCEPTION` blocks and external orchestrator (e.g., Airflow) retry mechanisms. This might represent a functional difference in how unexpected terminations are handled.
*   **Legacy Framework Functions:** The various `DWMSG_` and `DWDate_` functions from the original environment have been translated to BigQuery SQL logic (e.g., `INSERT` into `job_log`, `FORMAT_DATE`). While the general intent is preserved, specific nuances of these legacy functions (e.g., `DWMSG_ErmittleNr` for entry numbers) might require further refinement if exact behavior is critical. The `p_fehler_nr` parameter in `k_aurd_rechstan` is currently a placeholder and not actively used in the provided BigQuery logic.
*   **SQL in `D_AURD_RECHSTAN.SQL`:** The actual SQL logic from `D_AURD_RECHSTAN.SQL` was not provided in detail. The `k_aurd_rechstan` stored procedure contains placeholder `INSERT...SELECT` statements. The full and accurate translation of the original SQL, including all columns, joins, and specific WHERE clauses, is a critical step that needs to be completed and validated. The current code assumes `dwh_vertrag_id`, `gueltig_von`, `gueltig_bis`, `ladedatum`, and generic `col1, col2, col3`.

## 6. Validation

Validation involves comparing the output and behavior of the migrated BigQuery solution with the legacy KornShell job.

**How to Run Tests:**

1.  **Prepare Test Data:** Ensure `project.dataset.source_table` contains representative test data that mimics the production DWH contract cache tables. This should include scenarios for:
    *   Normal data extraction.
    *   Data that falls within/outside the `Stichtag` criteria.
    *   Data for restart scenarios (different `dwh_vertrag_id` values).
    *   Edge cases (e.g., empty source table, `ladedatum` values at `Stichtag` boundaries).
2.  **Execute Legacy Job:** Run the original `r_aurd_rechstan.ksh` script with specific `Stichtag` and `Wiederanlaufwert` parameters. Capture its output (target table data, log files).
    ```bash
    ./r_aurd_rechstan.ksh -s DDMMYYYY -l <restart_value>
    ```
3.  **Execute Migrated Job:** Call the `erzeugung_abzug_rechnungsdaten` BigQuery stored procedure with the *exact same* `Stichtag` and `Wiederanlaufwert` parameters.
    ```sql
    CALL `project.dataset.erzeugung_abzug_rechnungsdaten`('DDMMYYYY', <restart_value>);
    ```
    You can execute this via the BigQuery UI, `bq query` command, or an Airflow DAG if set up.
4.  **Repeat for Scenarios:** Run tests for various scenarios:
    *   **Full Run:** `p_input_stichtag` provided, `p_input_wiederanlaufWert = 0`.
    *   **Restart Run:** `p_input_stichtag` provided, `p_input_wiederanlaufWert > 0`.
    *   **Stichtag Fallback:** `p_input_stichtag = NULL` or `''`.
    *   **Error Handling:** Provide an invalid `p_input_stichtag` format.

**What "Passing" Means:**

A successful validation means the following criteria are met:

1.  **Data Equivalence:** The data in `project.dataset.target_table` after running the BigQuery stored procedure is *identical* to the data produced by the legacy job for the same input parameters. This includes:
    *   Number of rows.
    *   All column values matching exactly.
    *   Order of rows (if relevant, though usually not for DWH loads).
2.  **Restart Logic Fidelity:** When `p_input_wiederanlaufWert` is provided, the BigQuery job correctly deletes records from `target_table` and then inserts only the relevant new/reprocessed records, resulting in the same final state as the legacy job's restart mechanism.
3.  **Logging Accuracy:** The `project.dataset.job_log` table accurately reflects the execution flow, parameters, and status messages, mirroring the information captured in the legacy log files. Error messages should be clear and informative.
4.  **Status Updates:** The `project.dataset.job_status` table correctly updates the job's overall status (`OK`, `FAILED`, `RUNNING`) and last run details.
5.  **Parameter Handling:** The BigQuery stored procedure correctly parses and utilizes input parameters, including the `Stichtag` fallback logic, matching the behavior of the original script.
6.  **Error Propagation:** When an error occurs (e.g., invalid input, SQL error), the BigQuery procedure logs the error, updates the job status to `FAILED`, and raises an error that can be caught by an external orchestrator.

## 7. Rollback procedure

In case of issues detected during validation or after go-live, the following rollback procedure can be followed:

1.  **Stop New Migrated Job Runs:**
    *   If scheduled via Cloud Composer/Airflow, pause or disable the DAG for `erzeugung_abzug_rechnungsdaten`.
    *   Ensure no manual executions of the BigQuery stored procedure are initiated.
2.  **Revert to Legacy Job:**
    *   Re-enable the scheduling for the original `r_aurd_rechstan.ksh` script (e.g., re-activate its cron job).
    *   Verify that the legacy job can run successfully and produce the expected output.
3.  **Data Rollback (if necessary):**
    *   If the `project.dataset.target_table` was modified by the migrated job and its data is corrupted or incorrect, it might be necessary to revert it to a previous state.
    *   **Option A (Snapshot/Backup):** If a snapshot or backup of `project.dataset.target_table` was taken before the migrated job ran, restore the table from that snapshot.
    *   **Option B (Time Travel):** BigQuery's time travel feature can be used to query or restore the table to a point in time before the problematic run.
        ```sql
        -- To query data from a specific timestamp (e.g., 1 hour ago)
        SELECT * FROM `project.dataset.target_table` FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
        -- To restore the table to a specific timestamp (requires recreating the table)
        CREATE OR REPLACE TABLE `project.dataset.target_table` AS
        SELECT * FROM `project.dataset.target_table` FOR SYSTEM_TIME AS OF 'YYYY-MM-DD HH:MM:SS UTC';
        ```
    *   **Option C (Re-run Legacy Job):** If the legacy job can safely overwrite or correct the data in `project.dataset.target_table`, running the legacy job might be sufficient to restore data integrity.
4.  **Clean Up Migrated Artifacts (Optional):**
    *   If the rollback is permanent or requires a complete re-migration, consider dropping the BigQuery stored procedures and tables:
        ```sql
        DROP PROCEDURE IF EXISTS `project.dataset.erzeugung_abzug_rechnungsdaten`;
        DROP PROCEDURE IF EXISTS `project.dataset.k_aurd_rechstan`;
        DROP TABLE IF EXISTS `project.dataset.job_log`;
        DROP TABLE IF EXISTS `project.dataset.job_status`;
        -- Do NOT drop `project.dataset.target_table` unless it's confirmed to be only used by the migrated job and can be safely recreated.
        ```
5.  **Root Cause Analysis:** Investigate the reason for the rollback, address the identified issues (e.g., bugs in BigQuery SQL, incorrect parameter handling, missing data), and plan for re-migration.