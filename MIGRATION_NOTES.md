# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the KornShell script `k_ausd_bp_ta_apn_carmen.ksh` from a legacy Unix/Oracle environment to Google Cloud Platform, specifically Google BigQuery.

The original script served as an orchestrator, validating input parameters, performing date calculations, and executing an external SQL script (`d_ausd_bp_ta_apn_carmen.sql`) to process data related to `PoolBasisprodukt`. It also included error reporting and commented-out sections for job logging and file post-processing.

The migration targets Google BigQuery, where the orchestration logic, parameter validation, and error handling are encapsulated within a BigQuery Stored Procedure (`sp_k_ausd_bp_ta_apn_carmen`). The core data transformation logic from `d_ausd_bp_ta_apn_carmen.sql` will be translated into BigQuery SQL and integrated into this stored procedure. Logging will be handled by dedicated BigQuery tables (`job_error_log`, `job_control_log`). Any commented file manipulation logic (e.g., `sed`, `sort`, `join`) will be re-engineered using BigQuery SQL on staging tables if deemed active and necessary.

## 2. Generated Artifacts

The following files have been generated as part of this migration:

*   **`job_error_log.sql`**
    *   **Role:** BigQuery DDL script to create the `job_error_log` table. This table will store detailed information about any errors encountered during the execution of the BigQuery Stored Procedure, replacing the legacy `DWMSG_MeldeFehler` functionality.
*   **`job_control_log.sql`**
    *   **Role:** BigQuery DDL script to create the `job_control_log` table. This table will record metadata about each job execution, including start/end times, status, and record counts, replacing the commented `FOSJobErzeugeEintrag` functionality.
*   **`PoolBasisprodukt_target.sql`**
    *   **Role:** BigQuery DDL script to create the target table for the processed `PoolBasisprodukt` data. The schema is a placeholder and needs to be finalized based on the actual output of the translated `d_ausd_bp_ta_apn_carmen.sql`.
*   **`d_ausd_bp_ta_apn_carmen_bq.sql`**
    *   **Role:** Placeholder for the translated BigQuery SQL code that corresponds to the original `d_ausd_bp_ta_apn_carmen.sql` script. This file represents the core data processing logic. Its content will either be embedded directly into the `sp_k_ausd_bp_ta_apn_carmen` stored procedure or executed by it.
*   **`sp_k_ausd_bp_ta_apn_carmen.sql`**
    *   **Role:** BigQuery Stored Procedure that serves as the new orchestration layer. It encapsulates the parameter validation, date derivation, execution of the core data processing logic (from `d_ausd_bp_ta_apn_carmen_bq.sql`), and logging to the `job_error_log` and `job_control_log` tables. This procedure replaces the `k_ausd_bp_ta_apn_carmen.ksh` script.
*   **`cibasis_data24_staging.sql`**
    *   **Role:** BigQuery DDL script for a staging table. This is conditional and only required if the commented file processing logic involving `cibasis_data24` in the original ksh script is determined to be active and necessary.
*   **`cibasis_data96_staging.sql`**
    *   **Role:** BigQuery DDL script for a staging table. Conditional, similar to `cibasis_data24_staging.sql`, for `cibasis_data96`.
*   **`cibasis_fax_staging.sql`**
    *   **Role:** BigQuery DDL script for a staging table. Conditional, similar to `cibasis_data24_staging.sql`, for `cibasis_fax`.
*   **`cibasis_postprocessing_bq.sql`**
    *   **Role:** Placeholder for BigQuery SQL that translates the commented `sed`, `sort`, `join` operations from the original ksh script. This is conditional and only required if these operations are active and necessary. Its content would be executed by the main stored procedure.

## 3. Key Design Decisions

*   **BigQuery Stored Procedures for Orchestration:** The core orchestration logic of the KornShell script (parameter validation, date handling, error reporting, and calling the main SQL logic) is migrated into a BigQuery Stored Procedure. This centralizes the control flow within BigQuery, leveraging its native scripting capabilities and reducing external dependencies. This approach simplifies deployment and management compared to maintaining separate shell scripts and SQL files.
*   **BigQuery Tables for Logging:** Instead of file-based logging or proprietary logging mechanisms, dedicated BigQuery tables (`job_error_log`, `job_control_log`) are used. This provides a structured, queryable, and scalable logging solution, enabling easier monitoring, auditing, and analysis of job executions.
*   **Direct BigQuery SQL Translation:** The external SQL script (`d_ausd_bp_ta_apn_carmen.sql`) and any file manipulation utilities (`sed`, `sort`, `join`) are translated directly into BigQuery SQL. This eliminates the need for `sqlplus` or other external command-line tools, streamlining the data pipeline and leveraging BigQuery's performance for data transformations.
*   **Parameter-driven Execution:** The original script's command-line arguments are directly mapped to input parameters of the BigQuery Stored Procedure. This maintains the flexibility of the original job while integrating seamlessly into BigQuery's execution model.
*   **Standard BigQuery Functions for Utilities:** Common shell utilities like `gestern.ksh` (date derivation) and date format validation are replaced by native BigQuery SQL functions (`CURRENT_DATE()`, `DATE_SUB()`, `SAFE.PARSE_DATE()`). This reduces custom code and relies on robust, optimized BigQuery features.
*   **Trade-offs:**
    *   **Dependency on `d_ausd_bp_ta_apn_carmen.sql` translation:** The success of this migration heavily relies on the accurate and complete translation of the core SQL logic, which was not provided. Any complexity (e.g., dynamic SQL, PL/SQL constructs) in the original SQL will require significant manual effort.
    *   **Orchestration Complexity:** While the BigQuery Stored Procedure handles internal orchestration, if the job is part of a larger, more complex workflow with external system dependencies or intricate scheduling, an additional orchestration layer like Cloud Composer (Airflow) might be necessary, adding another layer of complexity.
    *   **Schema Inference:** The target `PoolBasisprodukt_target` table and staging tables have placeholder schemas. The actual schemas must be derived from the original `d_ausd_bp_ta_apn_carmen.sql` and any relevant data dictionaries.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:**
    *   Ensure the target BigQuery dataset (`project.dataset` in the generated code) exists. If not, create it:
        ```bash
        bq mk --location=US project:dataset
        ```
        (Replace `US` with the appropriate region).

2.  **IAM Permissions:**
    *   Grant the service account that will execute the BigQuery Stored Procedure the necessary IAM roles:
        *   `BigQuery Data Editor` on the target dataset (`project.dataset`) to create/update tables and execute stored procedures.
        *   `BigQuery Job User` to run BigQuery jobs.
        *   `BigQuery Data Viewer` on any source tables that `d_ausd_bp_ta_apn_carmen_bq.sql` reads from.

3.  **Deploy DDLs:**
    *   Execute the DDL scripts to create the logging and target tables:
        ```bash
        bq query --use_legacy_sql=false < job_error_log.sql
        bq query --use_legacy_sql=false < job_control_log.sql
        bq query --use_legacy_sql=false < PoolBasisprodukt_target.sql
        ```
    *   If the commented file processing logic is active, also deploy the staging table DDLs:
        ```bash
        bq query --use_legacy_sql=false < cibasis_data24_staging.sql
        bq query --use_legacy_sql=false < cibasis_data96_staging.sql
        bq query --use_legacy_sql=false < cibasis_fax_staging.sql
        ```

4.  **Translate and Deploy Core SQL Logic:**
    *   **Crucially, the placeholder `d_ausd_bp_ta_apn_carmen_bq.sql` must be replaced with the actual, fully translated BigQuery SQL from the original `d_ausd_bp_ta_apn_carmen.sql`.** This includes defining the correct source tables and the schema for `PoolBasisprodukt_target`.
    *   If `cibasis_postprocessing_bq.sql` is active, its placeholder must also be replaced with the translated BigQuery SQL.

5.  **Deploy BigQuery Stored Procedure:**
    *   Execute the `sp_k_ausd_bp_ta_apn_carmen.sql` script to create the stored procedure in BigQuery:
        ```bash
        bq query --use_legacy_sql=false < sp_k_ausd_bp_ta_apn_carmen.sql
        ```

6.  **Scheduling:**
    *   Configure a scheduling mechanism to invoke the BigQuery Stored Procedure. Options include:
        *   **BigQuery Scheduled Queries:** For simple, time-based scheduling.
        *   **Cloud Composer (Apache Airflow):** For complex workflows, dependency management, and integration with other GCP services or external systems. A DAG would be created to call the BigQuery Stored Procedure.
        *   **Cloud Workflows + Cloud Scheduler:** For event-driven or more flexible scheduling.

7.  **Source Data Ingestion:**
    *   Ensure all source tables referenced by the translated `d_ausd_bp_ta_apn_carmen_bq.sql` are present and populated in BigQuery. This might involve separate data ingestion pipelines (e.g., Cloud Data Transfer, Dataflow, or custom scripts).

## 5. Known Gaps & Unresolved References

The following items are identified as gaps or require further follow-up:

*   **Complexity of `d_ausd_bp_ta_apn_carmen.sql`:** The actual data transformation logic in `d_ausd_bp_ta_apn_carmen.sql` was not available for analysis. Its complexity (e.g., dynamic SQL, Oracle-specific constructs) will dictate the effort required for translation and may introduce unforeseen challenges. This is a **B4 item** requiring detailed analysis and translation.
*   **Missing `file_complexity` data:** The absence of complexity data for the original ksh script means potential hidden complexities or specific migration flags were not considered, which could impact the migration effort.
*   **Dynamic SQL/External Calls in `d_ausd_bp_ta_apn_carmen.sql`:** If the core SQL script contains dynamic SQL or calls to external systems, these will need specific re-engineering in BigQuery. The current design assumes static SQL.
*   **Job Purpose Clarity:** While inferred, a clearer understanding of the exact business purpose and dependencies of the original job from documentation would be beneficial to ensure complete and accurate migration.
*   **Active Status of Commented File Processing:** The `sed`, `sort`, `join` operations were commented out in the original script. It needs to be confirmed whether these operations are still active business requirements. If so, the placeholder DDLs for staging tables and the `cibasis_postprocessing_bq.sql` script must be completed and integrated.
*   **`PoolBasisprodukt_target` Schema:** The schema for the target table is a placeholder. It must be accurately defined based on the output of the translated `d_ausd_bp_ta_apn_carmen.sql`.

## 6. Validation

Validation involves executing the migrated BigQuery Stored Procedure and verifying its behavior and output.

1.  **Prepare Test Data:**
    *   Ensure that the BigQuery source tables used by `d_ausd_bp_ta_apn_carmen_bq.sql` contain representative test data for various scenarios (e.g., valid inputs, invalid dates, different `wiederanlaufWert` values).

2.  **Execute the Stored Procedure:**
    *   Call the `sp_k_ausd_bp_ta_apn_carmen` stored procedure with a range of test parameters, mimicking the original script's invocation:
        ```sql
        CALL `project.dataset.sp_k_ausd_bp_ta_apn_carmen`(
          p_JobKennung => 'TEST_JOB_CARMEN',
          p_EintragsNr => '001',
          p_Stichtag => '01012023', -- Example valid date
          p_wiederanlaufWert => 0
        );

        -- Example with a different restart value
        CALL `project.dataset.sp_k_ausd_bp_ta_apn_carmen`(
          p_JobKennung => 'TEST_JOB_CARMEN',
          p_EintragsNr => '002',
          p_Stichtag => '01012023',
          p_wiederanlaufWert => 100
        );

        -- Example with invalid date format (should log error and raise exception)
        CALL `project.dataset.sp_k_ausd_bp_ta_apn_carmen`(
          p_JobKennung => 'TEST_JOB_CARMEN',
          p_EintragsNr => '003',
          p_Stichtag => '2023-01-01', -- Invalid format
          p_wiederanlaufWert => 0
        );

        -- Example with missing mandatory parameter (should log error and raise exception)
        CALL `project.dataset.sp_k_ausd_bp_ta_apn_carmen`(
          p_JobKennung => NULL,
          p_EintragsNr => '004',
          p_Stichtag => '01012023',
          p_wiederanlaufWert => 0
        );
        ```

3.  **Verify "Passing" Conditions:**
    *   **Successful Execution:**
        *   The `PoolBasisprodukt_target` table should contain the expected processed data for successful runs.
        *   The `job_control_log` table should have a new entry for each successful execution, with `record_count` matching the number of rows inserted into `PoolBasisprodukt_target`.
        *   No new entries should appear in `job_error_log` for successful runs.
    *   **Error Handling:**
        *   For calls with invalid parameters or data, the stored procedure should terminate with an error (e.g., `SIGNAL SQLSTATE '45000'`).
        *   The `job_error_log` table should contain a new entry for each failed execution, detailing the `error_nr` and `error_msg`.
        *   The `job_control_log` table should *not* have an entry for failed runs (as the error occurs before successful completion logging).
    *   **Data Integrity:**
        *   Compare a sample of processed data in `PoolBasisprodukt_target` with the expected output from the legacy system (if possible) or manual verification based on the source data and transformation rules.
        *   Ensure date derivations (e.g., `v_datum_gestern`) are correct.
        *   Verify that the `v_resolved_wiederanlaufWert` is correctly applied in the core SQL logic.

## 7. Rollback Procedure

In case of issues during or after go-live, the following steps outline the procedure to roll back to the original KornShell script:

1.  **Stop New BigQuery Job Executions:**
    *   Immediately disable or delete any BigQuery Scheduled Queries, Cloud Composer DAGs, or Cloud Workflows that trigger the `sp_k_ausd_bp_ta_apn_carmen` stored procedure.

2.  **Re-enable Legacy Job:**
    *   Re-enable the original `k_ausd_bp_ta_apn_carmen.ksh` script in the legacy environment.
    *   Verify that the legacy job can run successfully and produce the expected output.

3.  **Data Cleanup (Optional but Recommended):**
    *   If the BigQuery job has written incorrect or partial data to `project.dataset.PoolBasisprodukt_target`, identify and delete the affected data. This might involve filtering by `created_at` timestamp or specific job identifiers.
        ```sql
        DELETE FROM `project.dataset.PoolBasisprodukt_target`
        WHERE created_at >= 'YYYY-MM-DD HH:MM:SS' -- Timestamp of migration start
          AND job_name = 'TEST_JOB_CARMEN'; -- Or other identifying columns
        ```
    *   Consider truncating the `PoolBasisprodukt_target` table if a full reset is desired and no other processes depend on its current state.

4.  **Remove BigQuery Objects (Optional):**
    *   Once the legacy system is stable and the rollback is confirmed, the migrated BigQuery objects can be removed to avoid confusion and resource consumption.
        ```bash
        bq rm -f project.dataset.sp_k_ausd_bp_ta_apn_carmen
        bq rm -f project.dataset.PoolBasisprodukt_target
        bq rm -f project.dataset.job_control_log
        bq rm -f project.dataset.job_error_log
        # If applicable, remove staging tables
        bq rm -f project.dataset.cibasis_data24_staging
        bq rm -f project.dataset.cibasis_data96_staging
        bq rm -f project.dataset.cibasis_fax_staging
        ```
    *   Note: `d_ausd_bp_ta_apn_carmen_bq.sql` and `cibasis_postprocessing_bq.sql` are typically not deployed as standalone BigQuery objects but rather embedded or executed.

5.  **Root Cause Analysis:**
    *   Investigate the reason for the rollback using the `job_error_log` table and BigQuery job logs to identify and resolve the issues before attempting re-migration.