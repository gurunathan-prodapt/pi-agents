# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the KornShell (ksh) control script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs2.ksh` and its associated SQL script `d_ausd_v_ta_cntrct_crs2.sql`. The migration targets Google BigQuery, transforming the shell-based orchestration and Oracle SQL data processing into BigQuery Stored Procedures and tables.

The original script's responsibilities included:
*   Parsing command-line parameters.
*   Validating parameters.
*   Managing job status in a central job table (ignoring active jobs, deactivating old ones).
*   Executing a core SQL script (`d_ausd_v_ta_cntrct_crs2.sql`).
*   Capturing and reporting processed record counts.
*   Basic error handling.

The migrated solution encapsulates this functionality within BigQuery Stored Procedures, leveraging BigQuery's native scripting capabilities for control flow, parameter management, and data manipulation.

## 2. Generated Artifacts

The migration process generated the following BigQuery-compatible files:

*   **`ddl/job_table.sql`**
    *   **Role:** Defines the Data Definition Language (DDL) for the `project.dataset.job_table`. This table serves as the central repository for tracking job executions, statuses, and metadata, replacing the legacy job table interaction.
*   **`ddl/error_log.sql`**
    *   **Role:** Defines the DDL for the `project.dataset.error_log` table. This table centralizes error and informational logging for BigQuery procedures, replacing the shell script's console output and custom error messaging.
*   **`ddl/sof_ta_cntrct_crs2.sql`**
    *   **Role:** Defines the DDL for the target data table `project.dataset.sof_ta_cntrct_crs2`. This table is where the processed contract data will be stored, inferred from the `INSERT` statement in the translated `d_ausd_v_ta_cntrct_crs2.sql` logic.
*   **`bq_sp/p_ausd_v_ta_cntrct_crs2_data_logic.sql`**
    *   **Role:** Contains the BigQuery Stored Procedure `project.dataset.p_ausd_v_ta_cntrct_crs2_data_logic`. This procedure encapsulates the core data processing logic originally found in `d_ausd_v_ta_cntrct_crs2.sql`, including data selection, transformation, and insertion into `sof_ta_cntrct_crs2`. It also handles record counting and internal error logging.
*   **`bq_sp/k_ausd_v_ta_cntrct_crs2.sql`**
    *   **Role:** Contains the main BigQuery Stored Procedure `project.dataset.k_ausd_v_ta_cntrct_crs2`. This procedure replaces the original `k_ausd_v_ta_cntrct_crs2.ksh` script, handling parameter validation, job status management (using `job_table`), calling the data processing procedure (`p_ausd_v_ta_cntrct_crs2_data_logic`), and comprehensive error handling and logging.

## 3. Key Design Decisions

The following key design decisions guided this migration:

*   **BigQuery Stored Procedures for Orchestration and Data Logic:** The entire control flow and data processing logic were translated into BigQuery Stored Procedures. This decision centralizes the job execution within the BigQuery environment, eliminating the need for external shell scripts and leveraging BigQuery's native scripting capabilities for parameter handling, conditional logic, and error management.
*   **Separation of Concerns:** The original pattern of a shell script calling a SQL script was maintained by creating two distinct BigQuery Stored Procedures:
    *   `k_ausd_v_ta_cntrct_crs2`: Handles orchestration, parameter validation, and job status.
    *   `p_ausd_v_ta_cntrct_crs2_data_logic`: Focuses solely on the core data transformation.
    This modular approach improves readability, maintainability, and reusability.
*   **Dedicated BigQuery Tables for Job Control and Error Logging:** Instead of relying on temporary files or implicit database interactions, explicit BigQuery tables (`job_table` and `error_log`) were created. This provides a structured, queryable, and centralized mechanism for monitoring job status, tracking processed records, and logging errors, aligning with modern data warehousing practices.
*   **Direct Translation of SQL Logic:** The Oracle SQL within `d_ausd_v_ta_cntrct_crs2.sql` was directly translated to BigQuery SQL, including:
    *   `NVL` to `IFNULL`.
    *   Oracle's `TO_CHAR(date, 'YYYYMMDD')` to BigQuery's `FORMAT_DATE('%Y%m%d', date)`.
    *   Oracle's proprietary outer join syntax `(+)` to standard `LEFT JOIN`.
    This approach minimizes changes to the core data transformation logic while adapting it to the target platform.
*   **BigQuery Scripting for Control Flow:** Shell script constructs like `getopts`, `if` conditions, and sourcing utility scripts were replaced by BigQuery's scripting features (e.g., `DECLARE`, `SET`, `IF`, `RAISE`, `EXCEPTION WHEN ERROR`). This ensures that the control flow is managed natively within BigQuery.
*   **Elimination of Temporary Files:** The original script's use of temporary files for inter-process communication (e.g., record counts) was replaced by BigQuery scripting variables (`DECLARE v_records INT64;`) and direct updates to the `job_table`, simplifying the architecture and removing file system dependencies.
*   **Robust Error Handling:** BigQuery's `EXCEPTION WHEN ERROR` blocks and `RAISE` statements were implemented to provide structured error handling, capturing error messages and logging them to the `error_log` table, which is a significant improvement over basic shell error checks.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:** Ensure the target BigQuery dataset (`project.dataset`) exists. If not, create it:
    ```bash
    bq mk --dataset project:dataset
    ```
2.  **Source Table Migration:**
    *   The source tables `project.dataset.dwtk_meldungen` and `project.dataset.sof_ta_cntrct_crs` must be migrated to BigQuery and populated with data. Their DDLs are not part of this migration but are critical prerequisites.
3.  **Deploy DDLs:** Execute the generated DDL scripts to create the necessary tables:
    ```bash
    bq query --use_legacy_sql=false < ddl/job_table.sql
    bq query --use_legacy_sql=false < ddl/error_log.sql
    bq query --use_legacy_sql=false < ddl/sof_ta_cntrct_crs2.sql
    ```
4.  **Deploy Stored Procedures:** Execute the generated Stored Procedure scripts:
    ```bash
    bq query --use_legacy_sql=false < bq_sp/p_ausd_v_ta_cntrct_crs2_data_logic.sql
    bq query --use_legacy_sql=false < bq_sp/k_ausd_v_ta_cntrct_crs2.sql
    ```
5.  **IAM Permissions:**
    *   The Google Cloud service account or user identity that will execute these BigQuery Stored Procedures must have appropriate IAM roles. Recommended roles include:
        *   `BigQuery Data Editor` on `project.dataset` (to create/update tables and insert/update data).
        *   `BigQuery Job User` (to run BigQuery jobs, including stored procedures).
        *   `BigQuery Metadata Viewer` (to view table schemas, if needed).
6.  **Scheduling Configuration:**
    *   If using Cloud Composer (Airflow), create a DAG that calls `project.dataset.k_ausd_v_ta_cntrct_crs2` with the required parameters (`p_job_kennung`, `p_eintrags_nr`).
    *   If using Cloud Workflows, define a workflow that orchestrates the procedure call.
    *   Alternatively, for simpler scheduling, BigQuery Scheduled Queries can be configured to call the main procedure.
    *   Ensure any external orchestration passes the `p_job_kennung` and `p_eintrags_nr` parameters correctly.

## 5. Known Gaps & Unresolved References

The following items are flagged for follow-up or represent areas where further analysis/development might be required:

*   **`dwtk_meldungen` and `sof_ta_cntrct_crs` Table Schemas:** The DDLs for these source tables were not part of this migration. Their exact schemas and data types in BigQuery must be confirmed and created before the procedures can run successfully. Any data type mismatches between the legacy Oracle and BigQuery could lead to errors.
*   **Full Functionality of Sourced KSH Utilities:** The original ksh script sourced several utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`). While the directly used functionalities (parameter validation, date formatting, SQL execution) have been translated, the full breadth of these utilities' capabilities might not be entirely replicated. If any other part of the system relies on these utilities, their BigQuery equivalents or alternative solutions need to be developed.
*   **External Orchestration Implementation:** While the BigQuery Stored Procedures are ready, the actual external orchestration (e.g., Cloud Composer DAG, Cloud Workflows definition) is not part of this deliverable. This needs to be implemented separately to manage scheduling, dependencies, and external triggers in a production environment.
*   **`BERT_DROP_TEMP_TABLE` Dependency:** The `p_ausd_v_ta_cntrct_crs2_data_logic` procedure queries `dwtk_meldungen` for `MAX(m.timecreated)` where `m.job_kennung = 'BERT_DROP_TEMP_TABLE'`. This indicates a dependency on another job's execution history. The migration status and behavior of `BERT_DROP_TEMP_TABLE` need to be understood to ensure this dependency is correctly managed in the BigQuery environment.

## 6. Validation

To validate the successful migration and functionality of the BigQuery procedures:

1.  **Deployment:** Ensure all DDLs and Stored Procedures listed in Section 2 are successfully deployed to the target BigQuery dataset.
2.  **Test Data Setup:**
    *   Populate `project.dataset.dwtk_meldungen` and `project.dataset.sof_ta_cntrct_crs` with representative test data that mirrors production scenarios, including edge cases.
    *   Consider creating scenarios for:
        *   First-time run.
        *   Subsequent runs with new data.
        *   Runs where the job is already active (to test the "ignore active jobs" logic).
        *   Runs with invalid parameters.
3.  **Execution:** Execute the main orchestration procedure using a BigQuery client or a test orchestration tool:
    ```sql
    CALL project.dataset.k_ausd_v_ta_cntrct_crs2('TEST_JOB_K_AUSD', '12345');
    ```
    *Replace `'TEST_JOB_K_AUSD'` and `'12345'` with appropriate test job and entry identifiers.*
4.  **"Passing" Criteria:** A successful validation run meets the following criteria:
    *   **Procedure Completion:** The `CALL` statement completes without raising an unhandled error.
    *   **Job Status:** Query `project.dataset.job_table` to confirm an entry for `'TEST_JOB_K_AUSD'` and `'12345'` with `status = 'SUCCESS'`.
    *   **Record Count:** The `record_count` in the `job_table` entry should match the actual `COUNT(*)` from `project.dataset.sof_ta_cntrct_crs2` after the procedure execution.
    *   **Error Logging:** Query `project.dataset.error_log` to ensure there are no `severity = 'ERROR'` entries for the executed job. Informational messages (`severity = 'INFO'`) are expected.
    *   **Data Integrity:** The data loaded into `project.dataset.sof_ta_cntrct_crs2` must be verified against the expected output from the legacy system using the same input data. This is the most critical step and may require data comparison tools or manual spot checks.
    *   **Parameter Validation:** Test with missing/invalid parameters to ensure the `RAISE` statements and error logging function as expected.
    *   **Active Job Handling:** Run the procedure twice in quick succession with the same parameters to verify the "ignore active jobs" logic. The second run should log an `INFO` message and return without processing.

## 7. Rollback Procedure

In case of issues or a decision to revert, follow these steps to roll back the migration:

1.  **Disable New Scheduling:** Immediately disable or delete any new Cloud Composer DAGs, Cloud Workflows, or BigQuery Scheduled Queries that invoke the `project.dataset.k_ausd_v_ta_cntrct_crs2` procedure.
2.  **Re-enable Legacy Job:** Re-enable the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs2.ksh` script in the legacy environment.
3.  **Drop BigQuery Stored Procedures:**
    ```sql
    DROP PROCEDURE IF EXISTS project.dataset.k_ausd_v_ta_cntrct_crs2;
    DROP PROCEDURE IF EXISTS project.dataset.p_ausd_v_ta_cntrct_crs2_data_logic;
    ```
4.  **Drop BigQuery Tables (Optional, but Recommended for Clean Rollback):**
    *   **Caution:** Dropping `sof_ta_cntrct_crs2` will delete all data processed by the new job. Ensure any critical data is backed up or moved if needed.
    ```sql
    DROP TABLE IF EXISTS project.dataset.sof_ta_cntrct_crs2;
    DROP TABLE IF EXISTS project.dataset.job_table;
    DROP TABLE IF EXISTS project.dataset.error_log;
    ```
    *   If the `job_table` and `error_log` contain valuable historical data from other migrated jobs, consider retaining them or archiving their contents before dropping.
5.  **Verify Legacy System:** Confirm that the original ksh script is running correctly and processing data as expected.