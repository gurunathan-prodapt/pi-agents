# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the ETL workflow associated with `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_inv_assign.ksh`. The original job, an Oracle/KornShell-based process, was responsible for the reconciliation and update of contract assignment data within the `sof$ta_inv_assign` table.

The entire workflow, including its wrapper script (`r_ausd_v_ta_inv_assign.ksh`), core processing script (`k_ausd_v_ta_inv_assign.ksh`), and data manipulation SQL script (`d_ausd_v_ta_inv_assign.sql`), has been migrated to **Google BigQuery**. The target platform leverages BigQuery Stored Procedures for orchestration and data transformation, along with dedicated BigQuery tables for logging and status tracking.

## 2. Generated artifacts

The migration process generated the following BigQuery SQL files, which define the new components:

*   **`your_project_id.your_dataset_id.job_log.sql`**
    *   **Role:** DDL for a new BigQuery table to store general job execution logs (INFO, WARNING, ERROR messages). This replaces the file-based logging of the original KornShell scripts.
*   **`your_project_id.your_dataset_id.job_error_log.sql`**
    *   **Role:** DDL for a new BigQuery table dedicated to storing detailed error information, including error codes, messages, and stack traces. This centralizes error reporting.
*   **`your_project_id.your_dataset_id.job_table.sql`**
    *   **Role:** DDL for a new BigQuery table to track the status and metadata of job executions (e.g., `RUNNING`, `SUCCESS`, `FAILED`, start/end times, processed rows). This replaces the implicit job status tracking and "active job" logic of the legacy system.
*   **`your_project_id.your_dataset_id.DWMSG_MeldeFehler.sql`**
    *   **Role:** BigQuery Stored Procedure. Replicates the functionality of the legacy `f_alis_msgerr.ksh` by inserting detailed error information into `job_error_log` and `job_log`.
*   **`your_project_id.your_dataset_id.DWMSG_ErmittleNr.sql`**
    *   **Role:** BigQuery Stored Procedure. Generates a unique `entry_nr` for each job run, similar to how the legacy system might have managed job identifiers.
*   **`your_project_id.your_dataset_id.DWMSG_Logdateiname.sql`**
    *   **Role:** BigQuery Stored Procedure (Placeholder). In the BigQuery context, logging is table-based. This procedure serves as a conceptual replacement for generating log file names, though its primary function is now to acknowledge the change in logging strategy.
*   **`your_project_id.your_dataset_id.DWMSG_ErzeugeEintrag.sql`**
    *   **Role:** BigQuery Stored Procedure. Inserts general log messages into the `job_log` table, providing a standardized way to record job progress and events.
*   **`your_project_id.your_dataset_id.DWMSG_SetzeStichtagInfo.sql`**
    *   **Role:** BigQuery Stored Procedure. Manages the reporting date (`Stichtag`) for a job run, updating the `job_table` and logging the information.
*   **`your_project_id.your_dataset_id.DWMSG_Fehlerbehandlung.sql`**
    *   **Role:** BigQuery Stored Procedure. Centralized error handling routine. It logs the error using `DWMSG_MeldeFehler`, updates the job status to 'FAILED' in `job_table`, and re-raises the error to halt execution.
*   **`your_project_id.your_dataset_id.DWMSG_SetzeStatusOK.sql`**
    *   **Role:** BigQuery Stored Procedure. Updates the job status to 'SUCCESS' in `job_table` upon successful completion, recording the end time and processed row count.
*   **`your_project_id.your_dataset_id.k_ausd_v_ta_inv_assign.sql`**
    *   **Role:** BigQuery Core Transformation Stored Procedure. This procedure replaces `k_ausd_v_ta_inv_assign.ksh` and embeds the translated logic from `d_ausd_v_ta_inv_assign.sql`. It handles date determination, truncates the target table, and performs the data insertion with the specified filtering logic. It also integrates BigQuery-native error handling and logging.
*   **`your_project_id.your_dataset_id.Vertragsdatenabgleich.sql`**
    *   **Role:** BigQuery Wrapper Stored Procedure. This procedure replaces `r_ausd_v_ta_inv_assign.ksh`. It acts as the entry point for the job, handling initial setup, parameter parsing, logging initialization, and orchestrating the call to the core transformation procedure (`k_ausd_v_ta_inv_assign`).

## 3. Key design decisions

The migration to BigQuery involved several key design decisions to translate the KornShell/Oracle workflow effectively:

*   **In-Database Orchestration**: The original KornShell wrapper (`r_ausd_v_ta_inv_assign.ksh`) and core script (`k_ausd_v_ta_inv_assign.ksh`) have been directly translated into BigQuery Stored Procedures (`Vertragsdatenabgleich` and `k_ausd_v_ta_inv_assign`). This decision centralizes the entire ETL logic within BigQuery, reducing external dependencies and simplifying deployment and monitoring.
*   **Direct SQL Translation**: The Oracle PL/SQL logic from `d_ausd_v_ta_inv_assign.sql` was directly translated into BigQuery SQL and embedded within the `k_ausd_v_ta_inv_assign` stored procedure. This preserves the original data transformation logic with minimal changes, primarily adapting syntax (e.g., `NVL` to `COALESCE`, `TO_DATE` to `PARSE_DATE`).
*   **Centralized Logging and Status Management**: The disparate logging mechanisms (file-based logs, implicit status tracking) of the legacy system have been replaced by a structured, table-based logging framework in BigQuery (`job_log`, `job_error_log`, `job_table`). This provides a consistent, queryable, and scalable approach to monitoring job execution and errors. Helper stored procedures (`DWMSG_ErzeugeEintrag`, `DWMSG_MeldeFehler`, `DWMSG_SetzeStatusOK`, etc.) encapsulate these logging operations.
*   **BigQuery Native Error Handling**: The `set -eu` and `trap` mechanisms of KornShell, along with Oracle's `WHENEVER SQLERROR`, are replaced by BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks. This allows for robust error trapping and integration with the new logging framework.
*   **Replacement of Utility Scripts**: Generic KornShell utility scripts (e.g., for error messaging, parameter handling, date utilities) have been either re-implemented as BigQuery helper stored procedures (e.g., `DWMSG_Fehlerbehandlung`) or their functionality has been inlined into the main stored procedures where appropriate.
*   **Resolution of Database Links**: The Oracle database link `&v_carmen` has been resolved by directly referencing the corresponding BigQuery table (`your_project_id.your_dataset_id.cds$ta_inv_assignment`), assuming the linked source data has been migrated to BigQuery.
*   **Native DDL for Truncation**: The Oracle procedural call `isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_inv_assign')` has been replaced by the native BigQuery `TRUNCATE TABLE` statement, simplifying the DDL execution.

**Notable Trade-offs:**
*   **Orchestration Complexity**: While keeping orchestration within BigQuery Stored Procedures simplifies deployment for this self-contained job, for more complex workflows involving multiple systems or external services, an external orchestrator like Cloud Composer (Apache Airflow) might offer greater flexibility and visibility. For this specific job, the in-database approach was chosen for its simplicity and direct translation.
*   **`manual` Migration Bucket for SQL**: The original `d_ausd_v_ta_inv_assign.sql` was flagged for 'manual' migration. The direct translation assumes no hidden complexities that would require a redesign. Any specific Oracle features not directly translatable or performance issues with large datasets might necessitate further optimization in BigQuery.

## 4. Manual steps before go-live

Before deploying the generated BigQuery components and going live, the following manual steps are required:

1.  **BigQuery Project and Dataset Setup**:
    *   Ensure that `your_project_id` and `your_dataset_id` exist in your Google Cloud environment. Replace these placeholders in all generated `.sql` files with your actual project ID and dataset ID.
    *   The dataset should be configured with appropriate location and default table expiration settings.

2.  **Source Data Migration**:
    *   **`isbert_schema.dwtk_meldungen`**: The Oracle table `isbert_schema.dwtk_meldungen` must be migrated or replicated into BigQuery as `your_project_id.your_dataset_id.dwtk_meldungen`. Ensure the schema (especially `timecreated` and `job_kennung`) and data types are correctly mapped.
    *   **`cds$ta_inv_assignment`**: The Oracle table `cds$ta_inv_assignment` (accessed via the `&v_carmen` database link) must be migrated or replicated into BigQuery as `your_project_id.your_dataset_id.cds$ta_inv_assignment`. This is a critical dependency; the data from the "Carmen DB" must be available in BigQuery. Ensure `cntrct_id`, `inv_definition_id`, `insert_at`, `modified_at`, `valid_from`, `valid_to`, and `is_production` columns are correctly mapped.

3.  **Target Table Creation**:
    *   The target table `sof$ta_inv_assign` must be created in BigQuery as `your_project_id.your_dataset_id.sof$ta_inv_assign` with the schema:
        ```sql
        CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.sof$ta_inv_assign` (
          cntrct_id STRING,
          inv_definition_id STRING
        );
        ```
        (Adjust data types if `cntrct_id` or `inv_definition_id` are not strings in the source).

4.  **IAM and Permissions**:
    *   The Google Cloud service account or user identity that will execute these BigQuery stored procedures must have the following IAM roles:
        *   `BigQuery Data Editor` on `your_project_id.your_dataset_id` (to create/truncate/insert into tables).
        *   `BigQuery Data Viewer` on `your_project_id.your_dataset_id` (to read source tables).
        *   `BigQuery Job User` (to run BigQuery jobs, including stored procedures).

5.  **Deployment of Generated Artifacts**:
    *   Execute the DDL for `job_log`, `job_error_log`, and `job_table`.
    *   Execute the DDL for all helper stored procedures (`DWMSG_MeldeFehler`, `DWMSG_ErmittleNr`, etc.).
    *   Execute the DDL for the core stored procedure `k_ausd_v_ta_inv_assign`.
    *   Execute the DDL for the wrapper stored procedure `Vertragsdatenabgleich`.
    *   Ensure all `CREATE OR REPLACE` statements are run in the correct order (dependencies first).

6.  **Scheduling**:
    *   The `your_project_id.your_dataset_id.Vertragsdatenabgleich` stored procedure needs to be scheduled for execution. This can be done using:
        *   **Cloud Scheduler**: For simple time-based scheduling.
        *   **Dataform**: If the job is part of a larger data pipeline managed by Dataform.
        *   **Cloud Composer (Apache Airflow)**: For complex workflows, dependency management, and integration with other systems.
    *   The scheduler will need to pass the `p_job_id` (e.g., `'R_AUSD_V_TA_INV_ASSIGN'`) and `p_reporting_date` (e.g., `CURRENT_DATE()`) parameters to the wrapper procedure.

## 5. Known gaps & unresolved references

The following items were identified as potential gaps, risks, or areas requiring further attention:

*   **Missing Complexity Data**: The original files lacked complexity tier information. While a direct translation was performed, this absence means there might be unforeseen complexities or performance bottlenecks that were not captured during the initial analysis.
*   **`manual` Migration Bucket for SQL Script**: The `d_ausd_v_ta_inv_assign.sql` was flagged for 'manual' migration. Although a direct BigQuery translation was provided, this flag suggests that the original Oracle SQL might contain highly specific features, performance considerations, or data volume challenges that could necessitate a more nuanced redesign or optimization in BigQuery. The use of `WHENEVER SQLERROR CONTINUE` in the Oracle script implies specific error handling behavior that needs careful validation in BigQuery's `BEGIN...EXCEPTION` blocks.
*   **Database Link (`&v_carmen`) Resolution**: The design assumes that the data from the external Oracle system (implied by `&v_carmen`) has been fully migrated or is continuously replicated into `your_project_id.your_dataset_id.cds$ta_inv_assignment` in BigQuery. If this data source is still external, a robust data ingestion pipeline (e.g., Fivetran, Dataflow, Datastream) must be established and validated to ensure data freshness and consistency. This is a critical external dependency.
*   **Full Fidelity of Error Handling Framework**: While the core error handling logic has been translated to BigQuery stored procedures, the full fidelity of the original KornShell `f_alis_msgerr.ksh` and its interaction with the broader system's error reporting (e.g., alerts, notifications) needs to be thoroughly reviewed and potentially integrated with Google Cloud's monitoring and alerting services (e.g., Cloud Monitoring, Cloud Logging, Pub/Sub).
*   **Orchestration Beyond BigQuery Stored Procedures**: For future enhancements or integration into a larger data mesh, the current in-BigQuery orchestration might become a limitation. Consider Cloud Composer (Apache Airflow) for more advanced scheduling, dependency management, and cross-system orchestration if the job's scope expands.

## 6. Validation

To ensure the successful migration and correct functioning of the BigQuery job, follow these validation steps:

1.  **Deployment Verification**:
    *   Confirm that all BigQuery tables (`job_log`, `job_error_log`, `job_table`, `sof$ta_inv_assign`, `dwtk_meldungen`, `cds$ta_inv_assignment`) and stored procedures are successfully deployed in `your_project_id.your_dataset_id`.
    *   Verify the schema of `sof$ta_inv_assign` matches the expected structure.

2.  **Execution of the Migrated Job**:
    *   Manually trigger the wrapper stored procedure:
        ```sql
        CALL `your_project_id.your_dataset_id.Vertragsdatenabgleich`('R_AUSD_V_TA_INV_ASSIGN', CURRENT_DATE());
        ```
        (Adjust `CURRENT_DATE()` to a specific historical date if you need to compare with historical Oracle runs).

3.  **"Passing" Criteria**:
    *   **Job Status**:
        *   Query `your_project_id.your_dataset_id.job_table` for the latest run of `R_AUSD_V_TA_INV_ASSIGN`. The `status` column should be `'SUCCESS'`.
        *   The `end_time` should be populated, and `processed_rows` should reflect the number of rows inserted.
    *   **Logging**:
        *   Query `your_project_id.your_dataset_id.job_log` for the `job_id` and `entry_nr` of the latest run. There should be a sequence of `INFO` messages indicating the job's progress (e.g., "Starting core transformation...", "Truncating...", "Inserting...", "Job completed successfully.").
        *   Verify there are no `ERROR` level messages in `job_log` for the successful run.
        *   Query `your_project_id.your_dataset_id.job_error_log` for the `job_id` and `entry_nr`. This table should be empty for a successful run.
    *   **Data Validation**:
        *   **Row Count Comparison**: Compare the `COUNT(*)` of `your_project_id.your_dataset_id.sof$ta_inv_assign` after the BigQuery run with the `COUNT(*)` of the original Oracle `sof$ta_inv_assign` table after a corresponding run (for the same `v_datum`). The counts should match.
        *   **Data Integrity Check**: For a representative sample of `cntrct_id` and `inv_definition_id` values, verify that the data in BigQuery matches the Oracle source. Consider using `CHECKSUM` or `HASH` functions on key columns if a full row-by-row comparison is too resource-intensive.
        *   **Filtering Logic Verification**: Spot-check records in `sof$ta_inv_assign` to ensure the `WHERE` clause conditions (`insert_at`, `modified_at`, `valid_from`, `valid_to`, `is_production`) were correctly applied based on the `v_datum` used.
        *   **Empty Source Scenario**: Test the job with an empty `cds$ta_inv_assignment` table (or a filtered result that yields no rows) to ensure it handles zero rows gracefully and `sof$ta_inv_assign` remains empty.

## 7. Rollback procedure

In case of critical issues or unexpected behavior after go-live, the following rollback procedure should be initiated:

1.  **Immediate Action**:
    *   **Stop BigQuery Schedule**: Immediately disable or delete the BigQuery job schedule (e.g., in Cloud Scheduler, Dataform, or Cloud Composer) that triggers `Vertragsdatenabgleich`. This prevents further execution of the migrated job.

2.  **Data Rollback**:
    *   **Restore Target Table**: If the `your_project_id.your_dataset_id.sof$ta_inv_assign` table was modified by the BigQuery job, restore it to its state prior to the problematic run. This can be done by:
        *   Restoring from a BigQuery table snapshot or time travel (if configured).
        *   Re-running the original Oracle job (`r_ausd_v_ta_inv_assign.ksh`) to overwrite the BigQuery-generated data in the Oracle `sof$ta_inv_assign` table (if the Oracle table is still the system of record).
        *   If BigQuery is the system of record, restore from a backup or a known good state.

3.  **Code Rollback**:
    *   **Delete BigQuery Components**: Delete the deployed BigQuery stored procedures (`Vertragsdatenabgleich`, `k_ausd_v_ta_inv_assign`, and all `DWMSG_` helper procedures) and the logging tables (`job_log`, `job_error_log`, `job_table`) from `your_project_id.your_dataset_id`.

4.  **Revert Scheduling**:
    *   **Re-enable Original Job**: Re-enable the original Oracle job's scheduler (`r_ausd_v_ta_inv_assign.ksh`) to resume normal operations using the legacy system.

5.  **Analysis and Re-evaluation**:
    *   Thoroughly analyze the root cause of the rollback.
    *   Update the migration design, code, or manual steps as necessary.
    *   Perform additional testing in a non-production environment before attempting another go-live.