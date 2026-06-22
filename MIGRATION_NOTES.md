# MIGRATION_NOTES.md

## 1. Summary

This migration involved transforming the KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_apn.ksh` into a Google BigQuery-native solution.

The original script served as an orchestration wrapper for provisioning selected base products for the BERT system, handling parameter parsing, environment setup, basic logging, and invoking a core processing script (`k_ausd_bp_ta_bpr_apn.ksh`).

The migration target is Google BigQuery, leveraging BigQuery Stored Procedures for procedural logic and data manipulation, and BigQuery tables for data storage and structured logging. Orchestration is expected to be handled by Google Cloud Composer or Google Cloud Workflows.

## 2. Generated Artifacts

The migration produced the following BigQuery DDL and Stored Procedure definitions:

*   **`sql/ddl/job_audit.sql`**
    *   **Role:** Defines the `job_audit` BigQuery table. This table replaces the file-based logging mechanism of the original KornShell script, providing structured, queryable audit trails for job executions, including status, messages, and parameter values.
*   **`sql/ddl/contract_cache.sql`**
    *   **Role:** Defines the `contract_cache` BigQuery table. This table serves as the BigQuery equivalent of the `DWH.TA_C_VERTRAG` source table referenced in the original script's context. It is assumed to be populated by an upstream data ingestion process.
*   **`sql/ddl/fos_table.sql`**
    *   **Role:** Defines the `fos_table` BigQuery table. This table is the target for the processed "contract cache" data, equivalent to the `FOS-Tabelle` in the legacy environment. It stores the output of the core processing logic.
*   **`sql/stored_procedures/k_ausd_bp_ta_bpr_apn.sql`**
    *   **Role:** Implements the core data processing logic as a BigQuery Stored Procedure. This procedure encapsulates the `DELETE` and `INSERT` statements that filter and transform data from `contract_cache` into `fos_table`, based on the provided `Stichtag` and `Wiederanlaufwert`. It is called by the main orchestration procedure.
*   **`sql/stored_procedures/ausd_bp_ta_bpr_apn.sql`**
    *   **Role:** Implements the main orchestration and parameter handling logic as a BigQuery Stored Procedure. This procedure is the primary entry point for the job, analogous to the original `r_ausd_bp_ta_bpr_apn.ksh` wrapper. It handles parameter validation, defaulting, job auditing, and invokes the `k_ausd_bp_ta_bpr_apn` procedure for core processing.

## 3. Key Design Decisions

*   **KornShell to BigQuery Stored Procedures:** The primary decision was to re-platform the KornShell script to BigQuery Stored Procedures. This leverages BigQuery's native capabilities for procedural logic, scalability, and integration within the Google Cloud ecosystem, eliminating the need for a separate execution environment for the script.
*   **Separation of Concerns (Wrapper & Core Logic):** The original script's structure, with a wrapper (`r_ausd_bp_ta_bpr_apn.ksh`) invoking a core processing script (`k_ausd_bp_ta_bpr_apn.ksh`), was maintained. This translates to two distinct BigQuery Stored Procedures: `ausd_bp_ta_bpr_apn` for orchestration and `k_ausd_bp_ta_bpr_apn` for data transformation. This modularity improves readability, maintainability, and allows for independent testing of the core logic.
*   **Structured Logging via `job_audit` Table:** File-based logging (`tee -a $LogDatei`) was replaced with inserts into a dedicated `job_audit` BigQuery table. This provides a structured, queryable, and centralized audit trail, making it easier to monitor job executions, debug issues, and track historical runs compared to parsing log files.
*   **Native BigQuery Parameter Handling and Date Functions:** Shell `getopts` for parameter parsing and custom date utilities were replaced by BigQuery Stored Procedure `IN` parameters and native BigQuery SQL functions (`PARSE_DATE`, `CURRENT_DATE`, `FORMAT_DATE`). This simplifies the code and leverages optimized BigQuery functionalities.
*   **BigQuery Error Handling (`RAISE`):** The legacy `exit $ErrNr` error handling was replaced with BigQuery's `RAISE` statement within `BEGIN...EXCEPTION WHEN ERROR...END` blocks. This allows for explicit error signaling within the procedure and propagates errors to the calling orchestrator, which can then handle retries, alerts, or further logging.
*   **Orchestration Integration:** The design anticipates using Google Cloud Composer (Apache Airflow) or Google Cloud Workflows for scheduling and managing the execution of the BigQuery Stored Procedures. This provides robust orchestration capabilities, dependency management, and monitoring.

**Notable Trade-offs:**
*   **Loss of Direct Shell Environment Control:** Migrating from KornShell means losing direct access to the underlying operating system's environment variables and file system. This is mitigated by BigQuery's robust parameter passing, dataset/project structure, and cloud-native services like Secret Manager for sensitive configurations.
*   **Increased BigQuery Dependency:** The solution is now tightly coupled with BigQuery. While this offers performance and scalability benefits, it means less portability to other database platforms.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:**
    *   Ensure the BigQuery dataset `my_project.my_dataset` (or your chosen project/dataset) exists. If not, create it:
        ```bash
        bq mk --dataset my_project:my_dataset
        ```
2.  **IAM/Permissions Setup:**
    *   The service account or user executing the BigQuery Stored Procedures (e.g., via Cloud Composer, Cloud Workflows, or directly) must have the following IAM roles:
        *   `BigQuery Data Editor` on `my_project.my_dataset` (to create/update tables and insert/delete data).
        *   `BigQuery Job User` on `my_project` (to run BigQuery jobs, including stored procedures).
3.  **Upstream Data Population:**
    *   The `my_project.my_dataset.contract_cache` table must be created and populated with data equivalent to the legacy `DWH.TA_C_VERTRAG`. This is an upstream dependency and is outside the scope of this specific migration, but crucial for the job's functionality.
    *   Ensure the schema of `contract_cache` matches the expected input for `k_ausd_bp_ta_bpr_apn`.
4.  **Secrets Management (if applicable):**
    *   While not explicitly used in the generated code, if any sensitive parameters or configurations were to be introduced, they should be stored in Google Secret Manager and securely accessed by the orchestrator.
5.  **Orchestration Setup:**
    *   **Cloud Composer (Airflow):**
        *   Develop and deploy an Apache Airflow DAG to your Cloud Composer environment.
        *   The DAG should use the `BigQueryOperator` or `BigQueryExecuteQueryOperator` to call the `my_project.my_dataset.ausd_bp_ta_bpr_apn` stored procedure.
        *   Configure the `p_stichtag_str` and `p_wiederanlaufwert_int` parameters as Airflow variables, XComs, or directly in the DAG.
        *   Set up scheduling, retries, and alerting within the DAG.
    *   **Cloud Workflows:**
        *   Define a Cloud Workflow that executes the BigQuery Stored Procedure.
        *   Configure input parameters and error handling.
        *   Schedule the workflow using Cloud Scheduler.
6.  **Deployment of BigQuery Objects:**
    *   Execute the DDL scripts (`job_audit.sql`, `contract_cache.sql`, `fos_table.sql`) to create the tables.
    *   Execute the Stored Procedure scripts (`k_ausd_bp_ta_bpr_apn.sql`, `ausd_bp_ta_bpr_apn.sql`) to create or replace the procedures.

## 5. Known Gaps & Unresolved References

*   **Complexity and Automation Rate:** The original assessment of "Medium" complexity and "Semi-Auto" automation was based on general script characteristics due to missing data from `file_complexity` and `automation_rate` tables. There might be hidden complexities in the original script not fully captured, potentially leading to underestimation of migration effort.
*   **Content of `k_ausd_bp_ta_bpr_apn.ksh`:** The actual data transformation logic in the original `k_ausd_bp_ta_bpr_apn.ksh` was not fully analyzed as part of this job's `component_files`. The generated `k_ausd_bp_ta_bpr_apn` BigQuery Stored Procedure is based on a general understanding and placeholder logic. A thorough review and validation against the original `k_ausd_bp_ta_bpr_apn.ksh` is critical to ensure functional equivalence.
*   **Error Codes and Messages:** The original script used specific numeric error codes (e.g., `ErrNr=193`). While BigQuery's `RAISE` provides error messages, a formal mapping or centralized error message catalog might be required if specific error codes are needed for downstream systems or monitoring.
*   **`DWMSG_SetzeStatusOK` Equivalence:** The original script explicitly set a "status OK". In BigQuery, successful completion without `RAISE` implies success. If an explicit "OK" status is required in the `job_audit` table for every step, additional `UPDATE` statements would be needed beyond the current implementation.
*   **Column Mapping for `contract_cache` and `fos_table`:** The DDLs for `contract_cache` and `fos_table` include placeholder columns (`col_a`, `col_b`). A precise mapping of all columns from `DWH.TA_C_VERTRAG` and `FOS-Tabelle` (including data types and nullability) is required to ensure data integrity and functional equivalence.

## 6. Validation

Validation should confirm that the migrated BigQuery Stored Procedures produce the same results as the legacy KornShell script for various input scenarios.

**How to Run Tests:**

1.  **Manual Execution:**
    *   Execute the main stored procedure `my_project.my_dataset.ausd_bp_ta_bpr_apn` directly from the BigQuery console or `bq` command-line tool with different parameter combinations:
        *   `CALL my_project.my_dataset.ausd_bp_ta_bpr_apn('01012023', 0);` (Normal run, no restart)
        *   `CALL my_project.my_dataset.ausd_bp_ta_bpr_apn('15062023', 1000);` (Run with specific Stichtag and restart value)
        *   `CALL my_project.my_dataset.ausd_bp_ta_bpr_apn(NULL, 0);` (Stichtag defaulted to current date)
        *   `CALL my_project.my_dataset.ausd_bp_ta_bpr_apn('INVALID_DATE', 0);` (Test error handling for invalid Stichtag)
2.  **Orchestrator Execution:**
    *   Once the Cloud Composer DAG or Cloud Workflow is set up, trigger it with various parameters to simulate production runs.

**What "Passing" Means:**

1.  **Successful Execution:**
    *   The `CALL` statement for `ausd_bp_ta_bpr_apn` completes without raising an unhandled error.
    *   The `job_audit` table shows a new entry for the execution with `status = 'SUCCESS'` and an appropriate `end_timestamp`.
2.  **Data Integrity and Equivalence:**
    *   Query the `my_project.my_dataset.fos_table` after each test run.
    *   Compare the data in `fos_table` with the output generated by the legacy `FOS-Tabelle` for the *exact same input parameters* (`Stichtag`, `Wiederanlaufwert`).
    *   Verify row counts, specific column values, and overall data distribution.
    *   Pay close attention to edge cases:
        *   `Wiederanlaufwert = 0` (full run).
        *   `Wiederanlaufwert > 0` (restart scenario, ensuring correct `DELETE` and `INSERT` behavior).
        *   Date filtering logic (`gueltig_von`, `gueltig_bis`, `ladedatum`).
3.  **Error Handling Validation:**
    *   Test cases designed to fail (e.g., invalid `Stichtag` format) should result in:
        *   The `CALL` statement raising an error.
        *   The `job_audit` table showing `status = 'FAILED'` and a descriptive `message` for the corresponding `job_entry_number`.
4.  **Performance:**
    *   Monitor the execution time of the BigQuery Stored Procedures and compare it against the legacy script's runtime.

## 7. Rollback Procedure

In case of critical issues identified post-migration, the following rollback procedure can be followed:

1.  **Stop New Executions:**
    *   Immediately pause or disable the Cloud Composer DAG or Cloud Workflow that triggers the migrated BigQuery Stored Procedure (`ausd_bp_ta_bpr_apn`). This prevents further execution of the new logic.
2.  **Revert Orchestration:**
    *   Reconfigure the orchestrator (e.g., Cloud Composer DAG, Cloud Scheduler job) to point back to the original legacy KornShell script (`r_ausd_bp_ta_bpr_apn.ksh`) and its execution environment.
3.  **Data Rollback (if necessary):**
    *   **Option A (Truncate/Delete):** If the `fos_table` was populated incorrectly by the new procedure and a clean slate is required, truncate or delete the affected data from `my_project.my_dataset.fos_table`.
        ```sql
        TRUNCATE TABLE `my_project.my_dataset.fos_table`;
        -- OR, if only specific runs need to be reverted:
        DELETE FROM `my_project.my_dataset.fos_table` WHERE stichtag_lauf >= 'YYYY-MM-DD';
        ```
    *   **Option B (BigQuery Time Travel):** Leverage BigQuery's time travel capability to restore `fos_table` to a state before the problematic runs. This is effective for up to 7 days.
        ```sql
        CREATE OR REPLACE TABLE `my_project.my_dataset.fos_table` AS
        SELECT * FROM `my_project.my_dataset.fos_table` FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
        ```
        (Adjust the `INTERVAL` as needed to the desired rollback point).
4.  **Re-enable Legacy Job:**
    *   Once the orchestrator is reverted and data (if necessary) is rolled back, re-enable the legacy job in its original environment.
5.  **Post-Rollback Verification:**
    *   Confirm that the legacy job is running as expected and producing correct results.
    *   Analyze the `job_audit` table for insights into the failed migrated runs to identify the root cause for future remediation.