# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `k_ausd_bp_ta_iccid_einzeln.ksh` from a legacy on-premise environment to Google Cloud Platform (GCP). The original script served as an orchestration layer, handling parameter validation, date calculations, and the execution of an Oracle SQL script.

The migration targets Google BigQuery for the core logic and Google Cloud Composer (Apache Airflow) for orchestration. The KornShell script's functionality has been refactored into BigQuery Stored Procedures, and its scheduling and parameter passing are managed by an Airflow DAG.

**Key Changes:**
*   **Orchestration:** KornShell script replaced by a BigQuery Stored Procedure (`r_ausd_bp_ta_iccid_einzeln`) and an Airflow DAG.
*   **Core Logic:** The invoked Oracle SQL script (`d_ausd_bp_ta_iccid_einzeln.sql`) is migrated to a BigQuery Stored Procedure (`d_ausd_bp_ta_iccid_einzeln`).
*   **Database:** Oracle database interactions are replaced by BigQuery SQL operations.
*   **Error Handling & Logging:** Custom shell-based logging is replaced by inserts into dedicated BigQuery logging tables and BigQuery's `RAISE` statements for immediate failures.
*   **Scheduling:** Manual or cron-based scheduling is replaced by an Airflow DAG.

## 2. Generated Artifacts

The migration process generated the following files:

*   **`sql/d_ausd_bp_ta_iccid_einzeln.sql`**
    *   **Role:** BigQuery Stored Procedure. This procedure encapsulates the core data processing logic originally found in `d_ausd_bp_ta_iccid_einzeln.sql`. It performs a `TRUNCATE` on the target table `project.dataset.sof_ta_iccid_einzeln` and then `INSERT`s transformed data from `project.dataset.sof_ta_bpr_basis` based on various `CASE WHEN` conditions.
*   **`sql/create_error_log_table.sql`**
    *   **Role:** BigQuery DDL for the `error_log` table. This table is used by the main orchestration procedure (`r_ausd_bp_ta_iccid_einzeln`) to record any validation or execution errors.
*   **`sql/create_job_log_table.sql`**
    *   **Role:** BigQuery DDL for the `job_log` table. This table captures high-level job execution metadata, including status, record counts, and timestamps, replacing the functionality of the commented-out `FOSJobErzeugeEintrag` in the original script.
*   **`sql/create_process_log_table.sql`**
    *   **Role:** BigQuery DDL for the `process_log` table. This table stores a summary of each run, including job-specific parameters and the number of records processed.
*   **`sql/r_ausd_bp_ta_iccid_einzeln.sql`**
    *   **Role:** BigQuery Stored Procedure. This is the main orchestration procedure, replacing the original `k_ausd_bp_ta_iccid_einzeln.ksh` script. It handles parameter validation, date conversions, calls the `d_ausd_bp_ta_iccid_einzeln` procedure, captures the record count, and logs execution details to the `job_log` and `process_log` tables.
*   **`dags/k_ausd_bp_ta_iccid_einzeln_dag.py`**
    *   **Role:** Apache Airflow DAG. This Python script defines the Airflow workflow responsible for scheduling and executing the `r_ausd_bp_ta_iccid_einzeln` BigQuery Stored Procedure. It passes parameters, including a dynamically generated `p_Stichtag` from Airflow's execution date.

## 3. Key Design Decisions

*   **KornShell to BigQuery Stored Procedure for Orchestration:** The control flow, parameter validation, and external script calls of the original KornShell script are directly translated into a BigQuery Stored Procedure (`r_ausd_bp_ta_iccid_einzeln`). This centralizes the logic within BigQuery, leveraging its native scripting capabilities and reducing external dependencies.
*   **Oracle SQL to BigQuery Stored Procedure for Core Logic:** The data transformation logic from `d_ausd_bp_ta_iccid_einzeln.sql` is migrated into a separate BigQuery Stored Procedure (`d_ausd_bp_ta_iccid_einzeln`). This promotes modularity and reusability, allowing the core transformation to be called by other processes if needed.
*   **Parameter Handling via Stored Procedure Parameters:** Instead of `getopts` and shell variables, all input parameters are explicitly defined as `IN` parameters for the BigQuery Stored Procedures. This provides strong typing and clear interfaces.
*   **BigQuery Native Functions for Date Operations:** Shell script calls to `gestern.ksh` and `DWDate_Datum_Check` are replaced by BigQuery's `CURRENT_DATE()`, `DATE_SUB()`, and `SAFE.PARSE_DATE()` functions, simplifying date calculations and validation.
*   **Structured Logging and Error Handling:** Legacy `DWMSG_MeldeFehler` calls are replaced by `INSERT` statements into dedicated BigQuery logging tables (`error_log`, `job_log`, `process_log`) and `RAISE` statements for immediate, controlled failure, providing better observability and debugging.
*   **Airflow for Scheduling and Parameterization:** Google Cloud Composer (Airflow) is chosen for scheduling. The DAG dynamically generates the `p_Stichtag` parameter based on the execution date, ensuring consistency and automation.
*   **`TRUNCATE` for Restart Logic:** The `d_ausd_bp_ta_iccid_einzeln` procedure uses `TRUNCATE TABLE` to clear the target table `sof_ta_iccid_einzeln` before inserting new data. This implicitly handles restart scenarios by ensuring a clean slate for each run, mirroring common ETL patterns.
*   **Consolidation of `CASE WHEN` Statements:** The complex logic involving multiple `CASE WHEN` statements in the `d_ausd_bp_ta_iccid_einzeln` procedure is directly translated to BigQuery SQL, efficiently mapping source data to multiple target columns based on `bpr_id` and `slave_number`.
*   **Assumption of Source and Target Table Schemas:** The migration assumes the existence and schema compatibility of the source table (`project.dataset.sof_ta_bpr_basis`) and the target table (`project.dataset.sof_ta_iccid_einzeln`) in BigQuery.

## 4. Manual Steps Before Go-Live

Before deploying and running the migrated job, the following manual steps are required:

1.  **GCP Project and BigQuery Dataset Setup:**
    *   Ensure a GCP project is active and billing is enabled.
    *   Create the target BigQuery dataset (e.g., `your-gcp-project-id.dataset`) where all procedures and tables will reside. Replace `your-gcp-project-id` and `dataset` placeholders in the generated code with actual values.
2.  **IAM Permissions:**
    *   Grant the service account used by Airflow (or the user executing the procedures) appropriate BigQuery roles:
        *   `BigQuery Data Editor` on the target dataset for `INSERT`, `TRUNCATE`, and `SELECT` operations.
        *   `BigQuery Job User` to run jobs.
        *   `BigQuery Data Viewer` on any source tables (e.g., `sof_ta_bpr_basis`).
3.  **Create Logging Tables:**
    *   Execute the DDL statements from `sql/create_error_log_table.sql`, `sql/create_job_log_table.sql`, and `sql/create_process_log_table.sql` in BigQuery to create the necessary logging tables.
4.  **Create Target Data Table:**
    *   Create the target table `project.dataset.sof_ta_iccid_einzeln` in BigQuery. The schema for this table can be inferred from the `INSERT` statement in `sql/d_ausd_bp_ta_iccid_einzeln.sql`. It should have columns matching the output of the `SELECT` statement.
5.  **Data Ingestion (Prerequisite):**
    *   Ensure that the source data, specifically the `project.dataset.sof_ta_bpr_basis` table, is ingested into BigQuery and populated with the necessary data. This is a critical prerequisite for the procedures to function correctly. The schema of `sof_ta_bpr_basis` must match the columns referenced in `sql/d_ausd_bp_ta_iccid_einzeln.sql` (e.g., `cntrct_id`, `bpr_id`, `slave_number`, `iccid`, `imsi_mcc`, `imsi_mnc`, `imsi_hlr`, `imsi_si`, `valid_to`, `E_ID`, `CARD_TYPE_NAME`).
6.  **Deploy BigQuery Stored Procedures:**
    *   Execute the `CREATE OR REPLACE PROCEDURE` statements from `sql/d_ausd_bp_ta_iccid_einzeln.sql` and `sql/r_ausd_bp_ta_iccid_einzeln.sql` in BigQuery to create the stored procedures.
7.  **Airflow DAG Deployment:**
    *   Upload the `dags/k_ausd_bp_ta_iccid_einzeln_dag.py` file to your Cloud Composer environment's DAGs folder.
    *   **Important:** Update `project_id='your-gcp-project-id'` and `dataset_id='dataset'` in the DAG file with your actual GCP project ID and BigQuery dataset ID.
8.  **Airflow Configuration (if needed):**
    *   Verify that the Airflow environment has the necessary BigQuery connection configured (usually `google_cloud_default`).

## 5. Known Gaps & Unresolved References

*   **Source Table Schema Verification:** While the generated SQL for `d_ausd_bp_ta_iccid_einzeln` assumes the schema of `project.dataset.sof_ta_bpr_basis`, the actual DDL for this source table was not part of the migration scope. It is crucial to verify that the columns (`cntrct_id`, `bpr_id`, `slave_number`, `iccid`, `imsi_mcc`, `imsi_mnc`, `imsi_hlr`, `imsi_si`, `valid_to`, `E_ID`, `CARD_TYPE_NAME`) exist and have compatible data types in BigQuery.
*   **Target Table Schema Verification:** Similarly, the DDL for the target table `project.dataset.sof_ta_iccid_einzeln` is not explicitly generated, but inferred from the `INSERT` statement. Its creation (manual step 4) requires careful attention to column names and types.
*   **Error Handling Integration:** The current error handling logs to a BigQuery table and raises an error. If the legacy system had specific monitoring or alerting integrations for `DWMSG_MeldeFehler`, these will need to be re-established using GCP monitoring services (e.g., Cloud Logging, Cloud Monitoring, Alerting Policies).
*   **`starteSQLSkript` Implicit Behavior:** The original `starteSQLSkript` wrapper might have had implicit behaviors (e.g., specific error code handling, connection pooling, temporary file management) that are not fully replicated by a direct BigQuery Stored Procedure call. Any such subtle behaviors should be identified and addressed if critical.
*   **`semi_auto` Migration Flag:** The `semi_auto` flag indicates that manual review and potential adjustments are expected. This includes verifying the generated SQL logic against the original script's intent and ensuring all edge cases are covered.
*   **Commented-out Code:** The commented-out `sed`, `sort`, `join` sections in the original script are assumed to be obsolete. If any of this functionality is still required, it must be explicitly re-implemented in BigQuery SQL.

## 6. Validation

To validate the migrated job, follow these steps:

1.  **Prerequisites:** Ensure all manual steps (Section 4) are completed, including data ingestion into `project.dataset.sof_ta_bpr_basis`.
2.  **Manual Execution (BigQuery Console):**
    *   Execute the `r_ausd_bp_ta_iccid_einzeln` stored procedure directly in the BigQuery console with sample parameters:
        ```sql
        CALL `your-gcp-project-id.dataset.r_ausd_bp_ta_iccid_einzeln`(
          'TEST_JOB',
          '001',
          '01012023', -- Example Stichtag (DDMMYYYY)
          0
        );
        ```
    *   Test with invalid `p_Stichtag` (e.g., `'32012023'`) and missing parameters to verify error handling.
3.  **Airflow DAG Execution:**
    *   Trigger the `k_ausd_bp_ta_iccid_einzeln_migration` DAG manually from the Airflow UI.
    *   Monitor the DAG run for success or failure.
4.  **Verification of Results:**
    *   **Output Data:** Query `project.dataset.sof_ta_iccid_einzeln` to verify that data has been inserted correctly. Compare a sample of the output with expected results from the legacy system (if available).
    *   **Record Count:** Check the `record_count` in `project.dataset.job_log` and `records` in `project.dataset.process_log` for the latest run. This count should match the number of rows in `project.dataset.sof_ta_iccid_einzeln`.
    *   **Logging:**
        *   Query `project.dataset.job_log` to ensure a successful entry (`job_status = 'A'`) exists for the run.
        *   Query `project.dataset.process_log` for the run details.
        *   If any errors occurred during testing, query `project.dataset.error_log` to verify that error messages were captured correctly.
    *   **Parameter Passing:** Confirm that the `p_Stichtag` and other parameters are correctly interpreted and used by the BigQuery procedures.

**"Passing" means:**
*   The Airflow DAG completes successfully without errors.
*   The `r_ausd_bp_ta_iccid_einzeln` BigQuery Stored Procedure executes successfully.
*   The `project.dataset.sof_ta_iccid_einzeln` table is populated with the expected data, matching the logic of the original SQL script.
*   The `record_count` in the logging tables accurately reflects the number of rows processed.
*   No unexpected errors are logged in `project.dataset.error_log` or Cloud Logging.
*   Error conditions (e.g., missing parameters, invalid date format) are correctly caught, logged, and result in a raised error.

## 7. Rollback Procedure

In case of issues or unexpected behavior after go-live, the following rollback procedure can be executed:

1.  **Disable Airflow DAG:**
    *   In the Airflow UI, toggle off the `k_ausd_bp_ta_iccid_einzeln_migration` DAG to prevent further scheduled executions.
2.  **Revert to Legacy Script:**
    *   Re-enable the original `k_ausd_bp_ta_iccid_einzeln.ksh` script in the legacy environment's scheduler (e.g., cron).
3.  **Optional: Clean Up BigQuery Objects:**
    *   If necessary, drop the created BigQuery stored procedures and tables. This step is optional and depends on whether the BigQuery environment needs to be completely clean or if the objects can remain for future analysis/re-migration attempts.
        ```sql
        DROP PROCEDURE IF EXISTS `your-gcp-project-id.dataset.r_ausd_bp_ta_iccid_einzeln`;
        DROP PROCEDURE IF EXISTS `your-gcp-project-id.dataset.d_ausd_bp_ta_iccid_einzeln`;
        DROP TABLE IF EXISTS `your-gcp-project-id.dataset.sof_ta_iccid_einzeln`;
        DROP TABLE IF EXISTS `your-gcp-project-id.dataset.error_log`;
        DROP TABLE IF EXISTS `your-gcp-project-id.dataset.job_log`;
        DROP TABLE IF EXISTS `your-gcp-project-id.dataset.process_log`;
        ```
    *   **Note:** Dropping tables will result in data loss for the migrated job's output and logs. Ensure any critical data is backed up or no longer needed before performing this step.