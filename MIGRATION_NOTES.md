# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the KornShell orchestration script `k_ausd_v_ta_p_discount_rr.ksh` and its associated SQL script `d_ausd_v_ta_p_discount_rr.sql`.

The original KornShell script, responsible for parameter parsing, environment setup, error handling, and executing a data processing SQL script, has been migrated to **Google Cloud Composer (Airflow)**. The core data transformation logic, originally in an Oracle SQL script, has been translated into **Google BigQuery SQL**. The target table `ta_p_discount_rr` will now reside in **Google BigQuery**.

This migration leverages cloud-native services for improved scalability, reliability, monitoring, and maintainability.

## 2. Generated Artifacts

The migration process generated the following files:

*   **`d_ausd_v_ta_p_discount_rr.bq.sql`**
    *   **Role**: This file contains the core data transformation logic, translated from the original `d_ausd_v_ta_p_discount_rr.sql` to be compatible with Google BigQuery SQL syntax. It performs an `INSERT` operation into the `ta_p_discount_rr` table by joining `sof_ta_discount_rr`, `sof_ta_cntrct_crs`, and `sof_ta_cntrct_templ` tables.
*   **`ta_p_discount_rr_schema.sql`**
    *   **Role**: This file provides the Data Definition Language (DDL) for creating the `ta_p_discount_rr` table in Google BigQuery. It defines the table structure and data types based on the columns used in the migrated SQL.
*   **`k_ausd_v_ta_p_discount_rr_dag.py`**
    *   **Role**: This is the Airflow Directed Acyclic Graph (DAG) Python script. It replaces the original KornShell script, orchestrating the data processing workflow. It handles parameter validation, executes the BigQuery SQL script (`d_ausd_v_ta_p_discount_rr.bq.sql`) using the `BigQueryOperator`, and includes placeholders for job management logic.

## 3. Key Design Decisions

*   **Orchestration from KornShell to Airflow**:
    *   **Why**: Airflow provides a robust, scalable, and cloud-native platform for orchestrating complex data workflows. It offers superior scheduling, monitoring, logging, and error handling capabilities compared to a standalone KornShell script. It also allows for easier integration with other GCP services.
    *   **Trade-offs**: Requires rewriting shell-specific logic (parameter parsing, environment setup, utility calls) into Python and Airflow operators. This introduces a new technology stack (Python/Airflow) and a learning curve for maintenance.
*   **Data Processing from Oracle SQL to BigQuery SQL**:
    *   **Why**: BigQuery is a fully managed, serverless, and highly scalable data warehouse designed for analytics. Migrating the SQL logic to BigQuery aligns with a cloud-native data strategy, offering performance benefits and cost-effectiveness for large datasets.
    *   **Trade-offs**: Requires careful translation of SQL dialect, especially for Oracle-specific functions or syntax. Potential for subtle behavioral differences between database engines.
*   **Replacement of Legacy Shell Utilities**:
    *   **Why**: The original script relied on several custom KornShell utility scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`). These have been replaced by native Airflow features (logging, parameter handling) or standard Python libraries (date/time functions). The `h_alis_sqlplus.ksh` utility is replaced by the `BigQueryOperator`.
    *   **Trade-offs**: Each utility script needs to be analyzed individually to ensure its functionality is fully replicated or adequately replaced by Airflow/Python equivalents. This can be a time-consuming process.
*   **Elimination of Temporary Files**:
    *   **Why**: The original script used a local temporary file to store and retrieve record counts. In the BigQuery/Airflow environment, this is replaced by direct interaction with BigQuery job statistics or by performing a follow-up query on the target table, which is more efficient and cloud-native.
    *   **Trade-offs**: Requires understanding how to extract job metrics from BigQuery operations within Airflow, which might not be as straightforward as reading a local file.
*   **Job Management Logic**:
    *   **Why**: The original script included logic for ignoring active jobs, making entries in a job table, and deactivating old jobs. This functionality is critical for operational integrity. The migration includes placeholders for this logic within the Airflow DAG, to be implemented using BigQuery updates or a dedicated metadata service.
    *   **Trade-offs**: This logic needs to be explicitly re-implemented and tested, as Airflow's native scheduling and task management might not fully cover the specific business rules of the legacy job table.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps are required:

1.  **BigQuery Dataset Creation**: Ensure the BigQuery dataset (`your_bigquery_dataset`) exists in your BigQuery project (`your_bigquery_project`). If not, create it.
2.  **BigQuery Table Creation**:
    *   Execute the `ta_p_discount_rr_schema.sql` script in BigQuery to create the `ta_p_discount_rr` table.
    *   **Review and Adjust Schema**: Carefully review the inferred data types in `ta_p_discount_rr_schema.sql` and adjust them based on the actual data characteristics and your BigQuery schema design best practices (e.g., `STRING` to `INT64`, `FLOAT64`, `DATE`, `TIMESTAMP`, etc.).
    *   **Partitioning/Clustering**: If `ta_p_discount_rr` requires partitioning or clustering for performance or cost optimization, add the relevant `PARTITION BY` and `CLUSTER BY` clauses to the `CREATE TABLE` statement in `ta_p_discount_rr_schema.sql` before execution.
3.  **Source Table Availability**: Ensure the source tables (`sof_ta_discount_rr`, `sof_ta_cntrct_crs`, `sof_ta_cntrct_templ`) are present in the specified BigQuery dataset and contain the necessary data.
4.  **IAM Permissions**:
    *   The Google Cloud service account used by your Cloud Composer environment (Airflow) must have appropriate permissions to:
        *   Read from the source BigQuery tables.
        *   Write to the target BigQuery table (`ta_p_discount_rr`).
        *   Execute BigQuery jobs.
        *   Access any other GCP resources used by the DAG (e.g., Cloud Storage if intermediate files are used).
5.  **Airflow Connection**: Verify that the `google_cloud_default` connection is configured correctly in your Airflow environment, pointing to your GCP project.
6.  **DAG Deployment**: Upload the `k_ausd_v_ta_p_discount_rr_dag.py` file to your Cloud Composer environment's DAGs folder.
7.  **SQL File Deployment**: Ensure `d_ausd_v_ta_p_discount_rr.bq.sql` is accessible by the Airflow DAG. The simplest way is to place it in the same DAGs folder or a subfolder relative to the DAG file.
8.  **Configuration Variables**: Update the `BIGQUERY_PROJECT_ID` and `BIGQUERY_DATASET_ID` variables in `k_ausd_v_ta_p_discount_rr_dag.py` to match your environment.
9.  **Email Configuration**: Update `email_on_failure` in `default_args` within the DAG to a valid email address for notifications.
10. **Job Management Logic Implementation**: The `_update_job_management_tables` function in the DAG is a placeholder. This needs to be fully implemented to replicate the legacy job tracking and deactivation logic, likely involving BigQuery `INSERT`/`UPDATE` statements on a dedicated job status table.

## 5. Known Gaps & Unresolved References

The following items are flagged for follow-up or represent areas requiring further attention:

*   **Comprehensive SQL Translation Review**: The `d_ausd_v_ta_p_discount_rr.bq.sql` script is a direct translation. A thorough review is needed to ensure all Oracle-specific SQL features (if any were present in the original `d_ausd_v_ta_p_discount_rr.sql`) have been correctly re-written for BigQuery and that the logic remains functionally equivalent.
*   **Full Job Management Logic**: The `_update_job_management_tables` function in the DAG is currently a placeholder. The complete logic for "ignoring active jobs," "making an entry in a job table," and "deactivating old active jobs" needs to be fully designed, implemented, and tested. This may involve creating a dedicated BigQuery table for job status tracking and developing specific BigQuery SQL or Python logic to interact with it.
*   **Parent Script Dependency (`r_ausd_vertrag.ksh`)**: The design document notes that `k_ausd_v_ta_p_discount_rr.ksh` is a control script for `r_ausd_vertrag.ksh`. The migration strategy for `r_ausd_vertrag.ksh` needs to be understood and aligned with this DAG's implementation. The triggering mechanism for this DAG (e.g., manual, Cloud Scheduler, or another Airflow DAG) should reflect its role in the broader workflow.
*   **Complete Utility Script Migration**: While core functionalities of the legacy utility scripts are replaced, a full audit of all functions within `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, and `h_alis_sqlplus.ksh` is recommended to ensure no critical functionality has been overlooked. Any remaining specific logic not covered by Airflow's native features or standard Python libraries should be implemented in Python helper functions within the DAG or as separate modules.
*   **`v_datum` Logic**: The original SQL script included logic to derive `v_datum` from `isbert_schema.dwtk_meldungen`, but this variable was not used in the provided `INSERT` statement. If `v_datum` is used elsewhere in the broader legacy process (e.g., for partitioning, filtering, or reporting), its derivation and usage need to be re-evaluated and implemented in the Airflow DAG or BigQuery.
*   **Data Type Validation**: The data types in `ta_p_discount_rr_schema.sql` are initial suggestions. A thorough review with source system data profiling is essential to ensure correct and optimal BigQuery data types are used.
*   **Error Handling Details**: The Airflow DAG uses standard Airflow error handling. Review if any specific error codes or messaging from `f_alis_msgerr.ksh` need to be replicated or mapped to specific Airflow alerts/notifications.

## 6. Validation

To ensure the successful migration and correct functioning of the new Airflow DAG and BigQuery SQL, the following validation steps should be performed:

1.  **BigQuery SQL Validation**:
    *   Manually execute the `d_ausd_v_ta_p_discount_rr.bq.sql` script in the BigQuery console (after replacing placeholders like `your_bigquery_project`) with sample data in the source tables.
    *   **Passing Criteria**: The query should execute successfully without syntax errors and produce the expected results in the `ta_p_discount_rr` table.
2.  **Airflow DAG Syntax Check**:
    *   Upload `k_ausd_v_ta_p_discount_rr_dag.py` to the Cloud Composer DAGs folder. Airflow should parse it without errors.
    *   **Passing Criteria**: The DAG appears in the Airflow UI without a "broken" status.
3.  **Airflow DAG Dry Run (Test)**:
    *   Use the Airflow CLI (`airflow dags test k_ausd_v_ta_p_discount_rr_dag <execution_date> -c '{"job_kennung": "TEST_JOB", "eintrags_nr": "123"}'`) or trigger a manual run from the UI with sample configuration parameters (`job_kennung`, `eintrags_nr`).
    *   **Passing Criteria**: All tasks in the DAG should complete successfully (green status) in the Airflow UI. Logs should indicate correct parameter validation and BigQuery job submission.
4.  **Data Validation**:
    *   After a successful DAG run, query the `ta_p_discount_rr` table in BigQuery.
    *   Compare a representative sample of the processed data with the output from the legacy system for the same input.
    *   **Passing Criteria**: The data in BigQuery's `ta_p_discount_rr` table must be identical to the data produced by the legacy system for the same input parameters and source data. This includes row counts and column values.
5.  **Job Management Logic Validation (Once Implemented)**:
    *   Verify that the job status table (if created) is updated correctly at the start and end of the DAG run, reflecting the job's status and parameters.
    *   **Passing Criteria**: The job status table accurately reflects the lifecycle of the Airflow job, including start, completion, and any deactivation logic.
6.  **Error Handling Test**:
    *   Test the DAG with invalid parameters (e.g., missing `job_kennung` or `eintrags_nr`) to ensure the `validate_parameters` task correctly raises an `AirflowException` and the DAG fails as expected.
    *   **Passing Criteria**: The DAG fails gracefully with informative error messages, and configured alerts (e.g., email) are triggered.

## 7. Rollback Procedure

In case of issues or unexpected behavior after go-live, the following rollback procedure can be executed:

1.  **Deactivate New Airflow DAG**:
    *   In the Cloud Composer (Airflow) UI, toggle off the `k_ausd_v_ta_p_discount_rr_dag` to prevent further executions.
    *   Alternatively, remove the `k_ausd_v_ta_p_discount_rr_dag.py` file from the DAGs folder.
2.  **Reactivate Legacy KornShell Script**:
    *   Re-enable the original `k_ausd_v_ta_p_discount_rr.ksh` script in its legacy scheduling system.
3.  **Revert BigQuery Data (if necessary)**:
    *   **Option A (Time Travel)**: If the `ta_p_discount_rr` table was overwritten (`WRITE_TRUNCATE`) and the rollback is within the BigQuery time travel window (default 7 days), you can restore the table to a state before the problematic DAG run.
        ```sql
        CREATE OR REPLACE TABLE `your_bigquery_project.your_bigquery_dataset.ta_p_discount_rr` AS
        SELECT * FROM `your_bigquery_project.your_bigquery_dataset.ta_p_discount_rr` FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL X MINUTE);
        ```
        (Replace `X` with the appropriate minutes to go back).
    *   **Option B (Backup/Snapshot)**: If a backup or snapshot of the `ta_p_discount_rr` table was taken before the migration, restore the table from that backup.
    *   **Option C (Re-run Legacy Job)**: If the legacy job can safely re-process and overwrite the data, run the original `k_ausd_v_ta_p_discount_rr.ksh` script to repopulate `ta_p_discount_rr` with the correct data.
    *   **Note**: Given the `WRITE_TRUNCATE` disposition, a robust data rollback strategy is critical.
4.  **Revert BigQuery Schema (if modified)**:
    *   If the schema of `ta_p_discount_rr` was altered in a non-compatible way during migration, revert it to its previous state. This might involve dropping and recreating the table or using BigQuery's schema update capabilities.
5.  **Clean Up (Optional)**:
    *   Remove any temporary BigQuery tables or Cloud Storage objects created by the Airflow DAG during its execution, if they are not needed for debugging or auditing.

This procedure ensures a quick return to the previous stable state while allowing for investigation and resolution of the migration issues.