# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the `k_ausd_bp_ta_rn_vertrag.ksh` KornShell script, which orchestrates a data extraction and load process for `PoolBasisprodukt` data. The original job reads from Oracle tables `DWTK_MELDUNGEN` and `SOF$TA_RN_EINZELN` and writes to `SOF$TA_RN_VERTRAG`.

The migration targets Google Cloud Platform, specifically:
*   **Target Platform**: Google BigQuery for data storage and transformation.
*   **Orchestration**: BigQuery Stored Procedure for the core logic, with an optional Cloud Composer (Apache Airflow) DAG for external scheduling.

The primary goal was to re-implement the script's parameter validation, date handling, and core SQL transformation logic natively within BigQuery, eliminating dependencies on KornShell, SQLPlus, and Oracle-specific utilities.

## 2. Generated Artifacts

The migration process has generated the following files:

*   **`ddl/your_bq_dataset.SOF_TA_RN_VERTRAG.sql`**
    *   **Role**: BigQuery Data Definition Language (DDL) script to create the target table `SOF$TA_RN_VERTRAG`. This table will store the processed `PoolBasisprodukt` data. It includes an audit column `PROCESSING_TIMESTAMP`.
*   **`ddl/your_bq_dataset.error_log.sql`**
    *   **Role**: BigQuery DDL script to create a dedicated `error_log` table. This table is used by the migrated stored procedure to record any errors encountered during its execution, providing a centralized logging mechanism.
*   **`ddl/your_bq_dataset.job_tracking.sql`**
    *   **Role**: BigQuery DDL script to create a `job_tracking` table. This table captures metadata about each job execution, such as start time, job name, parameters, and records processed, serving as an audit trail.
*   **`procedures/your_bq_dataset.r_ausd_bp_ta_rn_vertrag.sql`**
    *   **Role**: The core BigQuery Stored Procedure. This procedure encapsulates the entire logic of the original `k_ausd_bp_ta_rn_vertrag.ksh` script. It handles parameter validation, date derivation, executes the main data transformation (equivalent to `d_ausd_bp_ta_rn_vertrag.sql`), and logs execution details and errors.
*   **`dags/r_ausd_bp_ta_rn_vertrag_dag.py`**
    *   **Role**: An optional Apache Airflow DAG (Python script) designed to orchestrate the execution of the `r_ausd_bp_ta_rn_vertrag` BigQuery Stored Procedure. It demonstrates how to pass parameters from an external scheduler to the BigQuery procedure.

## 3. Key Design Decisions

*   **KornShell to BigQuery Stored Procedure for Orchestration**:
    *   **Why**: To leverage BigQuery's native capabilities for procedural logic, parameter handling, and error management, aligning with a cloud-native data warehousing strategy. This eliminates the need for external shell environments and simplifies deployment and maintenance.
    *   **Trade-offs**: Requires re-implementing shell-specific utilities (e.g., `getopts`, date functions, file operations) using BigQuery SQL scripting constructs. The flexibility of shell scripting for file system interactions is lost, but this job primarily deals with database operations.
*   **Oracle SQL to BigQuery SQL for Data Transformation**:
    *   **Why**: Direct translation of the core SQL logic into BigQuery's dialect ensures optimal performance within the BigQuery engine and avoids data egress/ingress overhead.
    *   **Trade-offs**: Requires careful syntax translation, especially for Oracle-specific functions or constructs. The `DWPA_UTIL_SKRIPT` package needs specific attention, as its functionality might require custom BigQuery UDFs or procedures if it's more than a simple dynamic SQL executor.
*   **Internalization of Utility Scripts**:
    *   **Why**: Shell utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, etc.) were replaced by BigQuery's built-in functions and procedural logic. This reduces external dependencies, improves maintainability, and centralizes the logic within the database.
    *   **Trade-offs**: Requires thorough understanding of each utility's function to ensure accurate re-implementation in BigQuery SQL.
*   **Dedicated Logging and Tracking Tables**:
    *   **Why**: To provide a standardized, BigQuery-native mechanism for error logging and job execution tracking, replacing the original script's `DWMSG_MeldeFehler` and commented `FOSJobErzeugeEintrag` calls. This enhances observability and debugging.
    *   **Trade-offs**: Requires defining and maintaining these auxiliary tables.
*   **Optional Cloud Composer (Airflow) for Scheduling**:
    *   **Why**: Provides a robust, scalable, and managed orchestration platform for scheduling and monitoring the BigQuery Stored Procedure, integrating it seamlessly into broader data pipelines.
    *   **Trade-offs**: Introduces an additional component (Airflow) if not already in use, requiring setup and maintenance of the DAG. For simple, isolated runs, direct BigQuery procedure calls might suffice.

## 4. Manual Steps Before Go-Live

Before the migrated job can be run in production, the following manual steps are required:

1.  **GCP Project and BigQuery Dataset Setup**:
    *   Ensure `your_gcp_project` (e.g., `my-data-project`) and `your_bq_dataset` (e.g., `dw_reporting`) are created in your Google Cloud environment.
    *   **Action**: Create project and dataset if they don't exist.
    *   **Command Example**: `bq mk --project_id=my-data-project dw_reporting`

2.  **IAM Permissions**:
    *   The service account or user executing the BigQuery Stored Procedure (and potentially the Airflow DAG) must have appropriate BigQuery permissions.
    *   **Required Roles**:
        *   `BigQuery Data Editor` on `your_bq_dataset` (to write to `SOF$TA_RN_VERTRAG`, `error_log`, `job_tracking`).
        *   `BigQuery Data Viewer` on `your_bq_dataset` (to read from `DWTK_MELDUNGEN`, `SOF$TA_RN_EINZELN`).
        *   `BigQuery Job User` (to run jobs).
        *   If using Airflow: `Composer Worker` role, and the Airflow service account needs the above BigQuery roles.
    *   **Action**: Grant necessary IAM roles to the executing identity.

3.  **Migrate Source Data**:
    *   The source tables `DWTK_MELDUNGEN` and `SOF$TA_RN_EINZELN` must be present and populated with data in `your_gcp_project.your_bq_dataset`. This typically involves a separate data ingestion pipeline from the legacy Oracle system (e.g., using Cloud Data Fusion, Dataflow, or Storage Transfer Service).
    *   **Action**: Ensure `DWTK_MELDUNGEN` and `SOF$TA_RN_EINZELN` tables exist and contain the required data.

4.  **Create Target and Logging Tables**:
    *   Execute the DDL scripts to create the target table `SOF$TA_RN_VERTRAG`, the `error_log` table, and the `job_tracking` table in your BigQuery dataset.
    *   **Action**: Run the following DDLs, replacing placeholders:
        *   `bq query --use_legacy_sql=false < ddl/your_bq_dataset.SOF_TA_RN_VERTRAG.sql`
        *   `bq query --use_legacy_sql=false < ddl/your_bq_dataset.error_log.sql`
        *   `bq query --use_legacy_sql=false < ddl/your_bq_dataset.job_tracking.sql`

5.  **Deploy BigQuery Stored Procedure**:
    *   Deploy the `r_ausd_bp_ta_rn_vertrag` stored procedure to your BigQuery dataset.
    *   **Action**: Run the procedure DDL, replacing placeholders:
        *   `bq query --use_legacy_sql=false < procedures/your_bq_dataset.r_ausd_bp_ta_rn_vertrag.sql`

6.  **Address `DWPA_UTIL_SKRIPT` (if necessary)**:
    *   If the Oracle package `DWPA_UTIL_SKRIPT.runstatement()` contains complex logic beyond simple dynamic SQL execution, this logic must be re-implemented as a BigQuery UDF or another BigQuery Stored Procedure.
    *   **Action**: Analyze `DWPA_UTIL_SKRIPT` and implement any required BigQuery equivalents.

7.  **Deploy Airflow DAG (if using Cloud Composer)**:
    *   If external scheduling is required, deploy the `r_ausd_bp_ta_rn_vertrag_dag.py` to your Cloud Composer environment's DAGs folder. Remember to replace `your_gcp_project` and `your_bq_dataset` placeholders in the DAG file.
    *   **Action**: Upload `dags/r_ausd_bp_ta_rn_vertrag_dag.py` to your Composer environment.

## 5. Known Gaps & Unresolved References

The following items were identified during the migration design and require further investigation or follow-up:

*   **`DWPA_UTIL_SKRIPT` Functionality**: The exact internal logic of the Oracle package `DWPA_UTIL_SKRIPT.runstatement()` is not fully transparent from the provided context. The current BigQuery Stored Procedure assumes it's a simple dynamic SQL executor and has omitted a direct equivalent.
    *   **Impact**: If this package performs complex data manipulation, logging, or other critical operations, the migrated job might behave differently or miss functionality.
    *   **Follow-up**: A detailed analysis of `DWPA_UTIL_SKRIPT` is required. If complex logic is found, it must be re-implemented in BigQuery (e.g., as a UDF or another stored procedure) and integrated into `r_ausd_bp_ta_rn_vertrag`.
*   **Environment Variables (`BERT_DIR_ROOT`, `DW_DIR_UTL`)**: The original script uses several environment variables for directory paths. While these are typically replaced by BigQuery project/dataset references or constants, their full usage context was not explicitly detailed.
    *   **Impact**: If these variables influenced dynamic file paths or configurations not directly related to database objects, those aspects might be missed.
    *   **Follow-up**: Confirm that all relevant uses of these environment variables have been correctly translated into BigQuery-native constructs or configuration parameters.
*   **Commented-Out Code (`sed`, `sort`, `join`)**: The original KornShell script contains commented-out sections for file-based processing.
    *   **Impact**: While currently inactive, there's a slight risk these operations might be reactivated in the future or represent dormant requirements.
    *   **Follow-up**: Confirm with business stakeholders that these operations are permanently defunct. If not, their BigQuery SQL equivalents would need to be implemented.
*   **Standardized Error Logging and Job Tracking**: While dedicated tables (`error_log`, `job_tracking`) have been created, the specific error codes and messages (`ErrNr`, `ErrArg`) are basic.
    *   **Impact**: The granularity and detail of logging might not fully match the legacy system's `DWMSG_MeldeFehler` if it had a more sophisticated error taxonomy.
    *   **Follow-up**: Review the legacy error handling for `DWMSG_MeldeFehler` to ensure the BigQuery logging captures equivalent or improved detail.

## 6. Validation

Validation ensures the migrated job functions correctly and produces accurate results.

**How to Run Tests:**

1.  **Direct BigQuery Stored Procedure Execution (Manual/Ad-hoc)**:
    *   Open the BigQuery UI or use the `bq` command-line tool.
    *   Call the stored procedure with various parameters:
        ```sql
        CALL `your_gcp_project.your_bq_dataset.r_ausd_bp_ta_rn_vertrag`(
          'TEST_JOB',
          'ENTRY_001',
          '01012023', -- Example Stichtag (DDMMYYYY)
          '0'
        );
        ```
    *   Test with valid parameters, invalid `Stichtag` format, and missing required parameters to verify error handling.

2.  **Cloud Composer (Airflow) Execution (Scheduled/Automated)**:
    *   Ensure the `dags/r_ausd_bp_ta_rn_vertrag_dag.py` is deployed to your Composer environment.
    *   Trigger the `bq_r_ausd_bp_ta_rn_vertrag` DAG manually from the Airflow UI.
    *   Monitor the DAG run for success or failure.

**What "Passing" Means:**

*   **Successful Execution**: The BigQuery Stored Procedure completes without raising an unhandled error.
*   **Correct Data Output**:
    *   Query the target table: `SELECT * FROM `your_gcp_project.your_bq_dataset.SOF$TA_RN_VERTRAG` ORDER BY PROCESSING_TIMESTAMP DESC LIMIT 100;`
    *   Verify that the data inserted into `SOF$TA_RN_VERTRAG` matches the expected output based on the source data and the transformation logic.
    *   **Critical**: Compare a sample of the output data with the output generated by the legacy `k_ausd_bp_ta_rn_vertrag.ksh` script for the same input parameters.
*   **Accurate Record Count**:
    *   The `RecordsProcessed` output from the stored procedure (or logged in `job_tracking`) should match the number of records inserted into `SOF$TA_RN_VERTRAG` for that run.
    *   Compare this count with the record count reported by the legacy system.
*   **Correct Logging**:
    *   Check the `job_tracking` table for a successful entry corresponding to the run.
    *   Check the `error_log` table. For successful runs, it should be empty for that job. For runs with intentionally invalid parameters, the `error_log` should contain the expected error message.
*   **Parameter Handling**:
    *   Verify that missing required parameters (`p_JobKennung`, `p_EintragsNr`, `p_Stichtag`) correctly trigger an error and log it.
    *   Verify that an invalid `p_Stichtag` format (e.g., '20230101' instead of '01012023') correctly triggers an error.
    *   Verify `p_wiederanlaufWert` defaults to '0' if not provided.

## 7. Rollback Procedure

In the event of critical issues or unexpected behavior after go-live, the following rollback procedure can be executed:

1.  **Stop New Migrated Job Executions**:
    *   If using Cloud Composer, disable or un-deploy the `bq_r_ausd_bp_ta_rn_vertrag` DAG.
    *   If the job is triggered manually, cease all manual invocations of the BigQuery Stored Procedure.

2.  **Revert to Legacy System**:
    *   Activate the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_vertrag.ksh` script in its production schedule/environment.
    *   Ensure the legacy job can process data correctly from its original source and write to its original target.

3.  **Clean Up BigQuery Artifacts (Optional, but Recommended)**:
    *   **Truncate/Delete Data**: If the data in `your_gcp_project.your_bq_dataset.SOF$TA_RN_VERTRAG` is not needed for analysis or debugging, truncate or delete the data inserted by the migrated job.
        ```sql
        TRUNCATE TABLE `your_gcp_project.your_bq_dataset.SOF$TA_RN_VERTRAG`;
        -- Or, if you need to keep historical data, delete specific runs:
        -- DELETE FROM `your_gcp_project.your_bq_dataset.SOF$TA_RN_VERTRAG` WHERE PROCESSING_TIMESTAMP >= 'YYYY-MM-DD HH:MM:SS';
        ```
    *   **Drop Stored Procedure**:
        ```sql
        DROP PROCEDURE IF EXISTS `your_gcp_project.your_bq_dataset.r_ausd_bp_ta_rn_vertrag`;
        ```
    *   **Drop Logging Tables (Optional)**: If the `error_log` and `job_tracking` tables are only used by this migrated job and not shared, they can also be dropped.
        ```sql
        DROP TABLE IF EXISTS `your_gcp_project.your_bq_dataset.error_log`;
        DROP TABLE IF EXISTS `your_gcp_project.your_bq_dataset.job_tracking`;
        ```

4.  **Investigate and Remediate**: Analyze the root cause of the rollback and plan for re-migration or further development.