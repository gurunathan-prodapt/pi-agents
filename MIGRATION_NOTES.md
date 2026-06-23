# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_discount_rr.ksh` and its dependent SQL script `vobs/dw_source/isrpt/isbert/SQL/aufbereitung/sql/d_ausd_v_ta_discount_rr.sql`.

The original job served as an orchestration layer, handling parameter validation, job control (ignoring active jobs, deactivating old ones), and executing a core SQL script to process data for the `ta_discount_rr` table.

The job has been migrated to **Google Cloud BigQuery**, leveraging BigQuery Stored Procedures for both orchestration and data processing logic. Logging mechanisms have been replaced with dedicated BigQuery logging tables.

## 2. Generated Artifacts

The migration produced the following BigQuery SQL files:

*   **`bq_logging_tables.sql`**
    *   **Role:** Defines the Data Definition Language (DDL) for two BigQuery tables:
        *   `my_project.my_dataset.error_log`: Stores detailed error messages and context for job failures.
        *   `my_project.my_dataset.job_log`: Records the start, end, status, and metrics (e.g., records processed) for each job execution.
    *   **Replaces:** Ad-hoc logging and error reporting mechanisms within the original KornShell script.

*   **`bq_d_ausd_v_ta_discount_rr.sql`**
    *   **Role:** Implements the core data processing logic as a BigQuery Stored Procedure named `my_project.my_dataset.d_ausd_v_ta_discount_rr`. This procedure truncates and inserts data into the `my_project.my_dataset.ta_discount_rr` table based on several source tables and a `p_process_date` parameter. It also handles internal error logging and reports the number of processed records.
    *   **Replaces:** The SQL logic contained within the original `vobs/dw_source/isrpt/isbert/SQL/aufbereitung/sql/d_ausd_v_ta_discount_rr.sql` file.

*   **`bq_k_ausd_v_ta_discount_rr_control.sql`**
    *   **Role:** Implements the orchestration and control logic as a BigQuery Stored Procedure named `my_project.my_dataset.r_ausd_vertrag_control`. This procedure performs parameter validation, derives the processing date, calls the `d_ausd_v_ta_discount_rr` data processing procedure, and manages overall job logging (start, end, status, records processed).
    *   **Replaces:** The entire `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_discount_rr.ksh` KornShell script.

## 3. Key Design Decisions

*   **BigQuery Stored Procedures for End-to-End Logic:** Both the orchestration (KornShell) and data processing (SQL) components were migrated into BigQuery Stored Procedures. This centralizes the entire job execution within BigQuery, leveraging its native capabilities for procedural logic, error handling, and data manipulation, eliminating the need for external shell scripts or SQL*Plus.
*   **Elimination of Temporary Files for Inter-Process Communication:** The original script used a temporary file to pass the record count from the SQL execution back to the KornShell wrapper. In BigQuery, this has been replaced by `OUT` parameters in the data processing procedure and BigQuery variables within the control procedure, providing a more robust and efficient mechanism.
*   **Structured Logging in BigQuery Tables:** Instead of disparate log files and custom error reporting functions (`DWMSG_MeldeFehler`), dedicated BigQuery tables (`error_log`, `job_log`) are used. This provides a centralized, queryable, and structured repository for all job execution metadata and errors.
*   **Native BigQuery Parameter Handling:** Command-line arguments from the KornShell script are directly translated into input parameters for the BigQuery Stored Procedures, ensuring type safety and explicit interfaces.
*   **`TRUNCATE` and `INSERT` Pattern for `ta_discount_rr`:** Based on the generated code for `d_ausd_v_ta_discount_rr`, the target table `ta_discount_rr` is fully refreshed on each run. This implies a full data replacement strategy.
*   **Date Derivation from `dwtk_meldungen`:** The logic for determining `v_process_date` (originally `v_datum`) from the `dwtk_meldungen` table has been replicated in the BigQuery control procedure to maintain functional parity with the source system.

## 4. Manual Steps Before Go-Live

Before the migrated job can be run in production, the following manual steps are required:

1.  **BigQuery Project and Dataset Setup:**
    *   Ensure the BigQuery project (`my_project`) and dataset (`my_dataset`) exist. If not, create them.

2.  **Source Table Migration:**
    *   All source tables referenced in `bq_d_ausd_v_ta_discount_rr.sql` must be migrated to `my_project.my_dataset`. These include:
        *   `cds_ta_discount_bc_assoc`
        *   `cds_ta_discount`
        *   `cds_ta_care_description`
        *   `cds_ta_disc_vector`
        *   `cds_ta_disc_invoice_item`
    *   The `dwtk_meldungen` table, used for deriving the processing date, must also be migrated to `my_project.my_dataset`.

3.  **Target Table DDL Creation:**
    *   Create the target table `my_project.my_dataset.ta_discount_rr` with the appropriate schema. The DDL is not provided in the generated code but can be inferred from the `INSERT` statement in `bq_d_ausd_v_ta_discount_rr.sql`:
        ```sql
        CREATE TABLE IF NOT EXISTS `my_project.my_dataset.ta_discount_rr`
        (
            cntrct_id               STRING,
            discount_id             STRING,
            disc_vector_ty          STRING,
            cntrct_obj_version      INT64,
            cntrct_template_id      STRING,
            disc_invoice_item_id    STRING,
            rabatt                  STRING,
            rabatthoehe             FLOAT64, -- Assuming CALC_RULE_VALUE is numeric
            rabattierte_rech_pos    STRING
        );
        ```
        *Note: Data types for `rabatthoehe` (CALC_RULE_VALUE) should be confirmed based on source system DDL.*

4.  **Deploy Logging Tables:**
    *   Execute the `bq_logging_tables.sql` script to create the `error_log` and `job_log` tables in `my_project.my_dataset`.

5.  **Deploy Stored Procedures:**
    *   Execute `bq_d_ausd_v_ta_discount_rr.sql` to create the data processing stored procedure.
    *   Execute `bq_k_ausd_v_ta_discount_rr_control.sql` to create the control stored procedure.

6.  **IAM Permissions:**
    *   Ensure the service account or user that will execute the BigQuery stored procedures has the necessary IAM roles:
        *   `BigQuery Data Editor` on `my_project.my_dataset` (for `INSERT`, `UPDATE`, `TRUNCATE` operations on `ta_discount_rr`, `error_log`, `job_log`, and `dwtk_meldungen`).
        *   `BigQuery Job User` (for running BigQuery jobs/procedures).
        *   `BigQuery Data Viewer` on `my_project.my_dataset` (for `SELECT` operations on source tables).

7.  **Scheduling:**
    *   **Simple Scheduling:** If the job has no complex external dependencies, configure a BigQuery Scheduled Query to call `my_project.my_dataset.r_ausd_vertrag_control` with the required parameters.
    *   **Complex Orchestration:** If the original "ignore active jobs" or "deactivate old active jobs" logic implies complex scheduling or dependencies, a Cloud Composer (Apache Airflow) DAG should be developed and deployed to trigger `my_project.my_dataset.r_ausd_vertrag_control`.

## 5. Known Gaps & Unresolved References

*   **`starteSQLSkript` Full Functionality:** The exact implementation details of the original `starteSQLSkript` function (e.g., transaction management, advanced error recovery, or updates to a central job tracking table beyond record count) were not fully known. The migration assumes its primary role was SQL execution and record count capture. Any additional logic would need to be identified and incorporated.
*   **Original `d_ausd_v_ta_discount_rr.sql` Content:** The full complexity and specific Oracle features (e.g., PL/SQL blocks, proprietary functions, specific data types) within the original SQL file were inferred. While the generated BigQuery SQL is a direct translation of the provided `INSERT` statement, a thorough review against the original Oracle SQL is recommended to ensure complete functional equivalence and optimal BigQuery performance.
*   **Absence of `file_complexity` Data:** The lack of complexity metrics for the source script means potential hidden complexities or edge cases might not have been fully accounted for during the design phase.
*   **Orchestration Beyond Basic Control:** The original script's mentions of "ignore already active jobs" and "deactivating old active jobs" suggest a more sophisticated job control mechanism. The current BigQuery control procedure handles basic parameter validation and sequential execution. If these "active job" controls involve complex external dependencies or inter-job communication, a dedicated orchestration tool like Cloud Composer would be necessary, which is currently optional in the design.
*   **`dwtk_meldungen` Table Schema and Data:** The migration assumes the `dwtk_meldungen` table exists in BigQuery with a `job_kennung` and `timecreated` column, and that `MAX(m.timecreated)` for `job_kennung = 'BERT_DROP_TEMP_TABLE'` reliably provides the correct processing date. The exact schema and data population of this table are critical for correct date derivation.
*   **`ta_discount_rr` Target Table DDL:** The DDL for the target table was inferred from the `INSERT` statement. It needs to be explicitly created and verified against the source system's schema for `ta_discount_rr` to ensure correct data types and constraints.

## 6. Validation

To validate the migrated job, follow these steps:

1.  **Prerequisites:** Ensure all manual steps (Section 4) have been completed, including the creation of all necessary tables and deployment of stored procedures. Ensure source data is available in BigQuery.

2.  **Execute the Control Procedure:**
    *   Open the BigQuery console or use the `bq` command-line tool.
    *   Call the main control stored procedure, providing valid `p_job_kennung` and `p_eintrags_nr` values.
    *   Example:
        ```sql
        CALL `my_project.my_dataset.r_ausd_vertrag_control`('TEST_JOB_DISCOUNT_RR', '12345');
        ```

3.  **Verify Passing Criteria:**

    *   **Successful Execution:** The BigQuery job for the stored procedure call should complete without an unhandled error.
    *   **`job_log` Entry:**
        *   Query `my_project.my_dataset.job_log` for the `p_job_kennung` and `p_eintrags_nr` used in the test.
        *   A `SUCCESS` entry should be present, with `start_timestamp`, `end_timestamp`, and `records_processed` reflecting the execution.
        *   The `records_processed` count should accurately reflect the number of rows inserted into `ta_discount_rr`.
    *   **`error_log` Absence:**
        *   Query `my_project.my_dataset.error_log` for the `p_job_kennung` and `p_eintrags_nr` used.
        *   There should be **no entries** related to this successful execution.
    *   **Target Data Verification:**
        *   Query `my_project.my_dataset.ta_discount_rr`.
        *   Verify that the data inserted matches the expected output from the original source system for the corresponding processing date. Compare row counts and a sample of data.
    *   **Parameter Validation (Negative Test):**
        *   Attempt to call the procedure with missing or invalid parameters (e.g., `CALL `my_project.my_dataset.r_ausd_vertrag_control`(NULL, '12345');`).
        *   Verify that an error is raised, and an entry is logged in `my_project.my_dataset.error_log` with the appropriate error code and message.

## 7. Rollback Procedure

In case of issues or a decision to revert, follow these steps to roll back the migration:

1.  **Stop New Executions:**
    *   If using BigQuery Scheduled Queries, disable or delete the scheduled query for `r_ausd_vertrag_control`.
    *   If using Cloud Composer, undeploy or disable the corresponding DAG.

2.  **Reactivate Original Job:**
    *   Re-enable and re-schedule the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_discount_rr.ksh` script in the legacy environment.

3.  **Revert BigQuery Objects (Optional, but recommended for clean-up):**
    *   **Drop Stored Procedures:**
        ```sql
        DROP PROCEDURE IF EXISTS `my_project.my_dataset.r_ausd_vertrag_control`;
        DROP PROCEDURE IF EXISTS `my_project.my_dataset.d_ausd_v_ta_discount_rr`;
        ```
    *   **Clear/Drop Logging Tables:**
        ```sql
        TRUNCATE TABLE `my_project.my_dataset.job_log`;
        TRUNCATE TABLE `my_project.my_dataset.error_log`;
        -- Or, to completely remove:
        -- DROP TABLE IF EXISTS `my_project.my_dataset.job_log`;
        -- DROP TABLE IF EXISTS `my_project.my_dataset.error_log`;
        ```
    *   **Revert `ta_discount_rr` Data:** If the `ta_discount_rr` table was modified during testing, restore its data to the state prior to the migration attempt (e.g., from a backup or by re-running the original legacy process to populate it). Do not drop the table if it's used by other processes.

4.  **Monitor Legacy System:**
    *   Ensure the original job is running correctly and producing the expected output in the legacy environment.