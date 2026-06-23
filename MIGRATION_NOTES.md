# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_iccid_einzeln.ksh` and its associated core SQL logic (implicitly from `d_ausd_bp_ta_iccid_einzeln.sql`).

The original KornShell script served as an orchestration component, handling parameter parsing, validation, environment setup, execution of a core SQL script, and basic record counting. The implicit business purpose was to process data related to the `PoolBasisprodukt` entity.

The migration targets Google BigQuery, transforming the script's functionality into BigQuery native constructs:
*   **Orchestration and Parameter Handling:** Reimplemented as a main BigQuery Stored Procedure (`r_ausd_bp_ta_iccid_einzeln`).
*   **Core Data Processing:** The logic from `d_ausd_bp_ta_iccid_einzeln.sql` is migrated into a separate BigQuery Stored Procedure (`p_process_iccid_einzeln`).
*   **Utility Functions:** Common functions like date validation and error handling are converted into BigQuery User-Defined Functions (UDFs) or helper stored procedures.
*   **Logging:** A dedicated BigQuery table (`job_log`) and stored procedure (`p_log_job_entry`) replace temporary files and legacy logging mechanisms.
*   **Target Data Storage:** The processed data is stored in a new BigQuery table (`sof_ta_iccid_einzeln`).

For external scheduling, Cloud Composer (Apache Airflow) or Cloud Workflows are recommended to trigger the main BigQuery stored procedure.

## 2. Generated Artifacts

The migration process generated the following BigQuery SQL artifacts:

*   **`sql/ddl/job_log.sql`**
    *   **Role:** Defines the Data Definition Language (DDL) for the `job_log` table. This table is used to record the execution status, parameters, record counts, and any messages for each run of the migrated job. It replaces the temporary file-based record count and any legacy job management system entries.
*   **`sql/ddl/sof_ta_iccid_einzeln.sql`**
    *   **Role:** Defines the DDL for the target table `sof_ta_iccid_einzeln`. This table will store the processed data, which was previously the output of the `d_ausd_bp_ta_iccid_einzeln.sql` script. Its schema is derived from the expected output of the core processing logic.
*   **`sql/stored_procedures/p_log_job_entry.sql`**
    *   **Role:** A BigQuery stored procedure responsible for inserting entries into the `job_log` table. It standardizes how job execution details (status, messages, record counts) are recorded throughout the main orchestration procedure.
*   **`sql/user_defined_functions/f_is_date_check.sql`**
    *   **Role:** A BigQuery User-Defined Function (UDF) that validates if a given string can be successfully parsed into a date using a specified format. This UDF replaces the functionality of the original KornShell `DWDate_Datum_Check` utility.
*   **`sql/stored_procedures/p_process_iccid_einzeln.sql`**
    *   **Role:** This BigQuery stored procedure encapsulates the core data processing logic that was originally contained within `d_ausd_bp_ta_iccid_einzeln.sql`. It truncates the target table `sof_ta_iccid_einzeln` and then inserts new data based on transformations from the source table `sof_ta_bpr_basis`, returning the number of records processed.
*   **`sql/stored_procedures/r_ausd_bp_ta_iccid_einzeln.sql`**
    *   **Role:** This is the main orchestration BigQuery stored procedure. It replaces the `k_ausd_bp_ta_iccid_einzeln.ksh` script. It handles parameter validation, date calculations, calls the `p_process_iccid_einzeln` procedure for core logic, and manages logging of job status and errors using `p_log_job_entry`.

## 3. Key Design Decisions

*   **Orchestration Shift to BigQuery Stored Procedures:** The KornShell script's role as an orchestrator was fully migrated to a BigQuery Stored Procedure (`r_ausd_bp_ta_iccid_einzeln`). This centralizes control flow within BigQuery, leveraging its native procedural capabilities and eliminating the need for external shell environments.
    *   **Trade-off:** While this simplifies the deployment and execution model within GCP, it moves some operational logic from a general-purpose scripting language to a database-specific procedural language, which might have a steeper learning curve for non-SQL developers.
*   **Encapsulation of Core Logic:** The SQL logic from `d_ausd_bp_ta_iccid_einzeln.sql` was extracted into its own BigQuery Stored Procedure (`p_process_iccid_einzeln`). This promotes modularity, reusability, and easier testing of the core data transformation independently.
*   **Native BigQuery Utilities:** All shell-based utility functions (e.g., `DWDate_Datum_Check`, `gestern.ksh`) were replaced with BigQuery's built-in functions (e.g., `CURRENT_DATE()`, `DATE_SUB()`, `PARSE_DATE()`) or custom BigQuery UDFs (`f_is_date_check`) and helper stored procedures (`p_log_job_entry`). This reduces external dependencies and leverages BigQuery's optimized functions.
*   **Centralized Logging:** The disparate logging mechanisms (temporary files, `echo`, potential legacy job tables) were consolidated into a dedicated `job_log` BigQuery table managed by a helper stored procedure (`p_log_job_entry`). This provides a consistent, queryable, and scalable logging solution.
*   **Robust Error Handling:** The `DWMSG_MeldeFehler` functionality was replaced by BigQuery's `RAISE` statement within `r_ausd_bp_ta_iccid_einzeln`, coupled with logging error details to the `job_log` table. This ensures that failures are captured and propagated effectively.
*   **Parameter Handling:** Command-line arguments (`getopts`) were directly mapped to `IN` parameters of the main BigQuery stored procedure, simplifying the interface and type safety.
*   **Target Table Schema Derivation:** The schema for the `sof_ta_iccid_einzeln` target table was inferred from the transformation logic found in the original `d_ausd_bp_ta_iccid_einzeln.sql` (as represented in `p_process_iccid_einzeln`). This ensures data compatibility.
*   **Source Table Assumption:** The migration assumes the existence of a BigQuery table `your_project_id.your_dataset_id.sof_ta_bpr_basis` which contains the source data previously accessed by the original SQL script (implied `PoolBasisprodukt`).

## 4. Manual Steps Before Go-Live

Before deploying and running the migrated job, the following manual steps are required:

1.  **BigQuery Project and Dataset Creation:**
    *   Ensure that `your_project_id` and `your_dataset_id` (as referenced in the generated code) exist in your Google Cloud Project. If not, create them.
2.  **Source Data Migration:**
    *   **Migrate `PoolBasisprodukt` data:** The source table, identified as `sof_ta_bpr_basis` in the generated code, must be migrated from its original database (implied Oracle) to `your_project_id.your_dataset_id.sof_ta_bpr_basis` in BigQuery. This involves:
        *   Defining the BigQuery schema for `sof_ta_bpr_basis`.
        *   Performing a one-time historical data load.
        *   Setting up a mechanism for ongoing data synchronization (e.g., CDC, batch loads) if the source data is dynamic.
3.  **IAM Permissions:**
    *   **BigQuery Data Editor:** The service account or user executing these BigQuery stored procedures must have `BigQuery Data Editor` role (or equivalent custom roles) on `your_project_id.your_dataset_id` to create tables, stored procedures, UDFs, and perform DML operations (INSERT, TRUNCATE).
    *   **BigQuery Job User:** The executing identity also needs `BigQuery Job User` to run BigQuery jobs.
    *   **Cloud Composer/Workflows Permissions (if applicable):** If using Cloud Composer or Workflows for orchestration, ensure the respective service accounts have the necessary permissions to trigger BigQuery jobs.
4.  **Deployment of DDL and Stored Procedures:**
    *   Execute the DDL scripts (`sql/ddl/job_log.sql`, `sql/ddl/sof_ta_iccid_einzeln.sql`) to create the necessary tables.
    *   Execute the stored procedure and UDF scripts (`sql/stored_procedures/*.sql`, `sql/user_defined_functions/*.sql`) to create the functions in BigQuery.
5.  **Scheduling Configuration (if applicable):**
    *   If using Cloud Composer, create and deploy an Airflow DAG that calls the `your_project_id.your_dataset_id.r_ausd_bp_ta_iccid_einzeln` stored procedure, passing the required parameters (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`, `p_wiederanlaufWert`).
    *   If using Cloud Workflows, define a workflow that orchestrates the BigQuery stored procedure execution.
6.  **Secrets Management:**
    *   Review the original `.dw_init` and other sourced scripts for any sensitive information (e.g., database credentials, API keys). If any were present, ensure they are securely managed in GCP (e.g., using Secret Manager) and passed to the BigQuery job or orchestrator if needed. (No explicit secrets were identified in the provided ksh, but this is a general best practice).

## 5. Known Gaps & Unresolved References

*   **Completeness of `d_ausd_bp_ta_iccid_einzeln.sql` Migration:** The provided `p_process_iccid_einzeln.sql` represents a *sample* migration of the core SQL logic. While it provides a plausible structure, the full and accurate content of the original `d_ausd_bp_ta_iccid_einzeln.sql` was not available in the design document. Any complex joins, subqueries, or specific functions within the original SQL must be carefully reviewed and translated to BigQuery SQL.
*   **Source Table `sof_ta_bpr_basis` Schema:** The generated `p_process_iccid_einzeln.sql` assumes a specific schema for `sof_ta_bpr_basis` (e.g., `cntrct_id`, `bpr_id`, `iccid`, `imsi_mcc`, `imsi_mnc`, `imsi_hlr`, `imsi_si`, `valid_to`, `E_ID`, `CARD_TYPE_NAME`, `slave_number`). This schema must be accurately reflected in the BigQuery migration of the `PoolBasisprodukt` data.
*   **Legacy File Processing:** The design document noted commented-out `sed`, `sort`, `join` commands in the original script. If these were ever active or if the `d_ausd_bp_ta_iccid_einzeln.sql` script itself interacted with flat files, this functionality is *not* covered by the current BigQuery migration. Such requirements would necessitate refactoring using BigQuery's external tables, `LOAD DATA` statements, or potentially PySpark jobs on Dataproc/Serverless Spark.
*   **Dynamic SQL:** If the original `d_ausd_bp_ta_iccid_einzeln.sql` or the `starteSQLSkript` function involved dynamic SQL generation based on runtime parameters, this complexity needs explicit handling in BigQuery stored procedures using `EXECUTE IMMEDIATE`. The current migration assumes static SQL.
*   **Full `f_alis_msgerr.ksh` Functionality:** The `f_alis_msgerr.ksh` script likely had a specific error reporting framework. While `RAISE` and logging to `job_log` cover basic error capture, any advanced features (e.g., specific notification channels, error aggregation) would need to be reimplemented in the GCP ecosystem (e.g., Cloud Logging, Cloud Monitoring alerts, Pub/Sub notifications).
*   **`p_wiederanlaufWert` Usage:** The original script accepted `p_wiederanlaufWert` but its usage was not explicitly detailed in the design document or the generated code. The migrated `r_ausd_bp_ta_iccid_einzeln` procedure accepts this parameter but does not currently implement any specific restart logic. If restartability was a critical feature, this needs to be designed and implemented (e.g., using conditional logic based on `p_wiederanlaufWert` to skip already processed data or resume from a specific point).

## 6. Validation

To validate the successful migration and functionality of the BigQuery job:

1.  **Unit Testing:**
    *   **`f_is_date_check` UDF:** Test with valid and invalid date strings and formats to ensure correct boolean output.
    *   **`p_log_job_entry` SP:** Call the procedure with various parameters and verify that entries are correctly inserted into the `job_log` table.
    *   **`p_process_iccid_einzeln` SP:**
        *   Populate `your_project_id.your_dataset_id.sof_ta_bpr_basis` with sample data.
        *   Execute `p_process_iccid_einzeln` with a `p_data_date`.
        *   Verify that `sof_ta_iccid_einzeln` is truncated and populated correctly according to the transformation logic.
        *   Check the `p_records_processed` output parameter for accuracy.
2.  **Integration Testing (End-to-End):**
    *   **Execute `r_ausd_bp_ta_iccid_einzeln` with Valid Parameters:**
        *   Call the main stored procedure with valid `p_JobKennung`, `p_EintragsNr`, `p_Stichtag` (e.g., '01012023'), and `p_wiederanlaufWert` (e.g., 'N').
        *   **Passing Criteria:**
            *   The procedure completes without raising an error.
            *   The `job_log` table contains two entries for the execution: one with `status = 'RUNNING'` and one with `status = 'SUCCESS'`.
            *   The `SUCCESS` entry in `job_log` shows the correct `records` count.
            *   The `sof_ta_iccid_einzeln` table contains the expected processed data, matching the output of the original `d_ausd_bp_ta_iccid_einzeln.sql` script when run against the same source data.
            *   The number of rows in `sof_ta_iccid_einzeln` matches the `records` count in `job_log`.
    *   **Execute `r_ausd_bp_ta_iccid_einzeln` with Invalid Parameters:**
        *   Test with missing `p_JobKennung`, `p_EintragsNr`, `p_Stichtag`.
        *   Test with an invalid `p_Stichtag` format (e.g., '2023-01-01').
        *   **Passing Criteria:**
            *   The procedure should `RAISE` an error with an appropriate message.
            *   The `job_log` table should contain an entry with `status = 'FAILED'` and the `message` column detailing the error.
            *   No data changes should occur in `sof_ta_iccid_einzeln` for failed runs (due to `RAISE` within the transaction).
3.  **Performance Testing:**
    *   Run the job with production-like data volumes in `sof_ta_bpr_basis` to ensure it meets performance SLAs.
4.  **Data Quality Checks:**
    *   Compare a sample of processed data in `sof_ta_iccid_einzeln` against the output of the legacy system for data accuracy and completeness.

## 7. Rollback Procedure

In case of critical issues or failure during the migration or post-go-live, the following rollback procedure can be executed:

1.  **Halt New Executions:** Immediately stop any new scheduled executions of the BigQuery stored procedure (`r_ausd_bp_ta_iccid_einzeln`) in Cloud Composer or any other orchestrator.
2.  **Revert to Legacy System:** Reactivate the original KornShell script (`k_ausd_bp_ta_iccid_einzeln.ksh`) and its associated scheduling mechanism. Ensure it can resume processing from the last successfully processed state.
3.  **Data Restoration (if necessary):**
    *   If the BigQuery job corrupted or incorrectly processed data in `sof_ta_iccid_einzeln`, restore the `sof_ta_iccid_einzeln` table to its state before the problematic BigQuery job execution. This might involve:
        *   Restoring from a BigQuery table snapshot or backup.
        *   Re-running the last successful legacy job to regenerate the data.
    *   Review the `job_log` table for details on the last successful run and any errors.
4.  **Clean Up BigQuery Artifacts (Optional, for full rollback):**
    *   Drop the BigQuery stored procedures:
        ```sql
        DROP PROCEDURE IF EXISTS your_project_id.your_dataset_id.r_ausd_bp_ta_iccid_einzeln;
        DROP PROCEDURE IF EXISTS your_project_id.your_dataset_id.p_process_iccid_einzeln;
        DROP PROCEDURE IF EXISTS your_project_id.your_dataset_id.p_log_job_entry;
        ```
    *   Drop the BigQuery UDF:
        ```sql
        DROP FUNCTION IF EXISTS your_project_id.your_dataset_id.f_is_date_check;
        ```
    *   Drop the BigQuery tables:
        ```sql
        DROP TABLE IF EXISTS your_project_id.your_dataset_id.sof_ta_iccid_einzeln;
        DROP TABLE IF EXISTS your_project_id.your_dataset_id.job_log;
        ```
    *   (Note: The source table `sof_ta_bpr_basis` should generally not be dropped as it's a source for other processes).
5.  **Investigate and Rectify:** Analyze the root cause of the failure using the `job_log` entries, BigQuery logs, and any other available monitoring data. Plan for remediation and re-migration.