# MIGRATION_NOTES.md

## 1. Summary

This migration job involved the `k_ausd_v_ta_cntrct_templ.ksh` KornShell script, which orchestrates a data preparation process to update the `ta_cntrct_templ` table. The script handles environment setup, parameter parsing, error handling, and executes an external SQL script (`d_ausd_v_ta_cntrct_templ.sql`).

The migration target platform is Google Cloud Platform, specifically utilizing BigQuery. The original KornShell orchestration logic has been translated into a BigQuery Stored Procedure, and the core SQL logic from `d_ausd_v_ta_cntrct_templ.sql` has been converted to BigQuery SQL. Data storage and processing now occur entirely within BigQuery.

## 2. Generated artifacts

The migration process generated the following BigQuery SQL files:

*   **`sql/ddl/ta_cntrct_templ.sql`**
    *   **Role:** Defines the Data Definition Language (DDL) for the `ta_cntrct_templ` table in BigQuery. This table is the primary target for the data preparation process. It replaces the legacy `sof$ta_cntrct_templ` table.
*   **`sql/ddl/job_audit.sql`**
    *   **Role:** Defines the DDL for a new `job_audit` table in BigQuery. This table serves as a centralized logging mechanism, replacing the implicit job table updates and temporary file-based record counting from the legacy system. It tracks job execution status, start/end times, processed records, and error messages.
*   **`sql/d_ausd_v_ta_cntrct_templ_bq.sql`**
    *   **Role:** Contains the core data transformation and insertion logic, translated from the original `d_ausd_v_ta_cntrct_templ.sql` to BigQuery SQL syntax. This script is responsible for truncating the `ta_cntrct_templ` table and inserting new data based on joins and filters from source tables (`cds_ta_cntrct_template`, `cds_ta_care_description`, `dwtk_meldungen`).
*   **`sql/sp/k_ausd_v_ta_cntrct_templ_sp.sql`**
    *   **Role:** Defines the BigQuery Stored Procedure `k_ausd_v_ta_cntrct_templ_sp`. This procedure encapsulates the orchestration logic of the original `k_ausd_v_ta_cntrct_templ.ksh` script. It handles parameter validation, logs job status to `job_audit`, executes the `d_ausd_v_ta_cntrct_templ_bq.sql` logic, counts processed records, and manages error handling.

## 3. Key design decisions

*   **BigQuery Stored Procedure for Orchestration:** The procedural logic of the KornShell script (parameter parsing, error handling, sequential execution) was migrated to a BigQuery Stored Procedure.
    *   **Why:** This centralizes the entire job execution within BigQuery, leveraging its native capabilities for SQL execution, error handling, and variable management. It eliminates the need for external shell environments and simplifies deployment and monitoring within GCP.
    *   **Trade-offs:** Requires re-implementing shell-specific utilities (e.g., `getopts`, `f_alis_msgerr.ksh`) using BigQuery SQL procedural language constructs.
*   **BigQuery for Data Transformation and Storage:** The core SQL logic and target tables were migrated to BigQuery.
    *   **Why:** BigQuery offers a highly scalable, serverless, and cost-effective data warehousing solution, aligning with modern cloud data architectures. It provides high performance for analytical queries and large-scale data processing.
    *   **Trade-offs:** Requires translation of SQL dialect (likely Oracle to BigQuery SQL), which can involve syntax changes, function mapping, and potential re-architecting of complex queries.
*   **Dedicated `job_audit` Table:** A new BigQuery table was introduced for job auditing.
    *   **Why:** Replaces implicit job table updates and file-based record counting, providing a structured, queryable, and centralized log for all job executions, status, and metrics. This enhances observability and debugging.
    *   **Trade-offs:** Requires explicit `INSERT` and `UPDATE` statements within the stored procedure for logging, adding a small overhead.
*   **Inlining SQL Logic (for `d_ausd_v_ta_cntrct_templ_bq.sql`):** The core SQL logic is executed via `EXECUTE IMMEDIATE` within the stored procedure.
    *   **Why:** Keeps the entire job self-contained within a single BigQuery Stored Procedure, simplifying deployment and ensuring atomicity of the operation.
    *   **Trade-offs:** Can make the stored procedure definition long if the SQL is very complex. For extremely large or frequently changing SQL, externalizing it into a separate BigQuery script and calling it might be considered, but for this job, inlining is manageable.
*   **Replacement of File-based Temporary Data:** Temporary files for record counts (`$DW_DIR_UTL/tmpFile`) were replaced by BigQuery variables and direct `SELECT COUNT(*)` queries.
    *   **Why:** Eliminates filesystem dependencies, which are not native to BigQuery, and leverages BigQuery's in-memory variable capabilities for efficient data handling.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Project and Dataset Setup:**
    *   Ensure a Google Cloud Project is established.
    *   Create the necessary BigQuery datasets (e.g., `your_dataset_id`) within your project (`your_project_id`) to host the tables and stored procedures.
2.  **Schema Creation:**
    *   Execute the DDL scripts to create the required tables:
        *   `sql/ddl/ta_cntrct_templ.sql`
        *   `sql/ddl/job_audit.sql`
    *   Ensure that any source tables referenced in `d_ausd_v_ta_cntrct_templ_bq.sql` (e.g., `cds_ta_cntrct_template`, `cds_ta_care_description`, `dwtk_meldungen`) exist in the specified BigQuery datasets and are populated with relevant data.
3.  **IAM/Permissions:**
    *   Grant appropriate IAM roles to the service account or user that will execute the BigQuery Stored Procedure. This typically includes:
        *   `BigQuery Data Editor` on the dataset containing `ta_cntrct_templ` and `job_audit` (for `INSERT`, `UPDATE`, `TRUNCATE`).
        *   `BigQuery Data Viewer` on datasets containing source tables (e.g., `dwtk_meldungen`, `cds_ta_cntrct_template`, `cds_ta_care_description`).
        *   `BigQuery Job User` to run BigQuery jobs.
4.  **Connection Strings/Configuration:**
    *   The generated code uses placeholders like ``your_project_id.your_dataset_id``. These *must* be replaced with the actual project ID and dataset ID where the tables and stored procedures reside before deployment.
5.  **Stored Procedure Deployment:**
    *   Execute the `sql/sp/k_ausd_v_ta_cntrct_templ_sp.sql` script to create the BigQuery Stored Procedure. Remember to replace the `your_project_id.your_dataset_id` placeholders.
6.  **Scheduling and Orchestration:**
    *   Set up an external orchestrator (e.g., Google Cloud Composer/Airflow, Cloud Workflows, Cloud Scheduler calling a Cloud Function) to trigger the `k_ausd_v_ta_cntrct_templ_sp` BigQuery Stored Procedure.
    *   Configure the orchestrator to pass the required `p_JobKennung` and `p_EintragsNr` parameters to the stored procedure.

## 5. Known gaps & unresolved references

*   **Content of `d_ausd_v_ta_cntrct_templ.sql`:** The exact original SQL logic was inferred and translated. Any complex or highly specific Oracle SQL constructs not fully captured in the design document might require further review and adjustment in `d_ausd_v_ta_cntrct_templ_bq.sql`.
*   **Full Logic of Sourced Helper Scripts:** While the purpose of helper scripts like `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, and `h_alis_sqlplus.ksh` was identified, their full internal logic was not exhaustively re-implemented. The most critical aspects (parameter validation, error reporting) have been covered in the BigQuery Stored Procedure. Any subtle behaviors or specific date calculations from `h_alis_date.ksh` might need further BigQuery function mapping if they become apparent during testing.
*   **Job-Table Management Details:** The original "Job-table related behavior" was replaced by the generic `job_audit` table. If the legacy system had specific update patterns or additional fields in its job table, these would need to be incorporated into `job_audit` and the stored procedure's logging logic.
*   **Oracle-Specific SQL:** The migration assumes a relatively straightforward translation from Oracle-like SQL to BigQuery SQL. If `d_ausd_v_ta_cntrct_templ.sql` contained advanced Oracle features (e.g., specific analytic functions, PL/SQL blocks, custom types), these would require more complex BigQuery equivalents or re-design.
*   **Idempotency:** The original script's comment "aktive Jobs werden ignoriert" implies a mechanism to prevent concurrent runs. The current BigQuery Stored Procedure does not explicitly implement this. While BigQuery job execution is typically atomic, if concurrent calls are possible and undesirable, an external locking mechanism or a check within the `job_audit` table (e.g., `WHERE status = 'RUNNING'`) would be needed.

## 6. Validation

To validate the migrated job, follow these steps:

1.  **Prepare Test Data:** Ensure the source tables (`cds_ta_cntrct_template`, `cds_ta_care_description`, `dwtk_meldungen`) in BigQuery contain representative test data that mimics production scenarios, including edge cases (e.g., `NULL` values for `modified_at`, `valid_to`).
2.  **Execute the Stored Procedure:**
    *   Call the BigQuery Stored Procedure `k_ausd_v_ta_cntrct_templ_sp` with test parameters:
        ```sql
        CALL `your_project_id.your_dataset_id.k_ausd_v_ta_cntrct_templ_sp`('TEST_JOB_KENNUNG', 'TEST_ENTRY_NR');
        ```
    *   Test with missing parameters to ensure validation errors are raised:
        ```sql
        CALL `your_project_id.your_dataset_id.k_ausd_v_ta_cntrct_templ_sp`(NULL, 'TEST_ENTRY_NR');
        ```
3.  **Check BigQuery Job History:** Verify that the BigQuery job executed successfully in the GCP Console.
4.  **Inspect `job_audit` Table:**
    *   Query the `job_audit` table for the `TEST_JOB_KENNUNG` and `TEST_ENTRY_NR`.
    *   **Passing means:**
        *   An entry exists with `job_id = 'TEST_JOB_KENNUNG'` and `entry_number = 'TEST_ENTRY_NR'`.
        *   `status` is 'SUCCESS'.
        *   `start_time` and `end_time` are populated.
        *   `records_processed` accurately reflects the number of records inserted into `ta_cntrct_templ`.
        *   For failed runs (e.g., due to bad data or internal errors), `status` should be 'FAILED' and `error_message` should contain relevant details.
5.  **Verify `ta_cntrct_templ` Data:**
    *   Query the `ta_cntrct_templ` table:
        ```sql
        SELECT * FROM `your_project_id.your_dataset_id.ta_cntrct_templ`;
        ```
    *   **Passing means:**
        *   The table contains the expected number of records as indicated by `records_processed` in `job_audit`.
        *   The data in `cntrct_template_id`, `cds_description_id`, and `cds_description` columns matches the expected output based on the source data and the transformation logic.
        *   Perform a data comparison (e.g., row counts, checksums, or detailed record-by-record comparison) against the output of the legacy system for the same input data.

## 7. Rollback procedure

In case of issues with the migrated BigQuery job, the following rollback procedure can be executed:

1.  **Disable New Job:**
    *   Immediately disable or pause the scheduling mechanism (e.g., Cloud Composer DAG, Cloud Workflow, Cloud Scheduler job) that triggers the `k_ausd_v_ta_cntrct_templ_sp` BigQuery Stored Procedure.
2.  **Re-enable Legacy Job:**
    *   Re-enable the original `k_ausd_v_ta_cntrct_templ.ksh` KornShell script in its legacy environment.
    *   Verify that the legacy job can run successfully and process data as expected.
3.  **Data Reconciliation (if necessary):**
    *   If the BigQuery job partially ran or produced incorrect data in `ta_cntrct_templ` before rollback, assess the impact.
    *   Depending on the severity, it might be necessary to:
        *   Truncate the `ta_cntrct_templ` table in BigQuery.
        *   Manually re-run the legacy job to ensure the source system's target table is in a consistent state.
        *   Consider a full data reload from a trusted source if the `ta_cntrct_templ` table is critical and corrupted.
4.  **Investigate and Fix:**
    *   Analyze the `job_audit` table and BigQuery job logs to identify the root cause of the failure in the migrated job.
    *   Address the identified issues in the BigQuery DDL, SQL, or Stored Procedure code.
    *   Once fixes are implemented and thoroughly tested in a non-production environment, the migration can be re-attempted.