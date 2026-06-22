# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the KornShell script `k_ausd_v_ta_notice.ksh`, located at `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_notice.ksh`, to Google Cloud BigQuery.

The original script served as an orchestration wrapper, handling parameter parsing, job control, and execution of an underlying SQL script (`d_ausd_v_ta_notice.sql`). The migration translates this orchestration logic into BigQuery Stored Procedures and leverages BigQuery's native capabilities for logging and data processing.

**Target Platform:** Google Cloud BigQuery.

**What was migrated:**
*   The KornShell script's control flow, parameter handling, and logging mechanisms.
*   The execution of the core data processing logic (originally in `d_ausd_v_ta_notice.sql`) is now encapsulated within a BigQuery Stored Procedure.
*   Shell-based logging and temporary file communication have been replaced with dedicated BigQuery logging tables and stored procedure `OUT` parameters.

## 2. Generated Artifacts

The migration produced the following BigQuery-native artifacts:

*   **`ddl/job_error_log.sql`**
    *   **Role:** DDL script to create the `job_error_log` table. This table is used to record details of any errors encountered during the execution of BigQuery jobs, replacing the custom error logging framework from the original KornShell environment.
*   **`ddl/job_run_log.sql`**
    *   **Role:** DDL script to create the `job_run_log` table. This table tracks the start and completion status of BigQuery job runs, providing an audit trail similar to what might have been captured by shell script logging.
*   **`ddl/job_run_result.sql`**
    *   **Role:** DDL script to create the `job_run_result` table. This table stores the outcome of job runs, specifically the count of processed records, replacing the temporary file (`$DW_DIR_UTL/bert_k_ausd_v_ta_notice_$$.tmp`) used for inter-script communication in the original setup.
*   **`ddl/ta_notice.sql`**
    *   **Role:** DDL script to create the target `ta_notice` table. This table is where the data processed by `sp_d_ausd_v_ta_notice` will be stored. Its schema is derived from the expected output of the original `d_ausd_v_ta_notice.sql` script.
*   **`sp/sp_d_ausd_v_ta_notice.sql`**
    *   **Role:** BigQuery Stored Procedure (`sp_d_ausd_v_ta_notice`). This procedure encapsulates the core data transformation logic that was originally present in `d_ausd_v_ta_notice.sql`. It reads from a source table (`cds_ta_notice`), truncates and inserts data into `ta_notice`, and returns the count of processed records.
*   **`sp/sp_control_ta_notice.sql`**
    *   **Role:** BigQuery Stored Procedure (`sp_control_ta_notice`). This is the main control procedure, replacing the `k_ausd_v_ta_notice.ksh` script. It handles parameter validation, logs job status to `job_run_log`, determines the processing date, calls `sp_d_ausd_v_ta_notice` to execute the core logic, logs the processed record count to `job_run_result`, and manages error handling by logging to `job_error_log`.

## 3. Key Design Decisions

*   **Orchestration in BigQuery Stored Procedures:** The KornShell script's role as an orchestrator was migrated directly into a BigQuery Stored Procedure (`sp_control_ta_notice`). This keeps the control logic close to the data and leverages BigQuery's native procedural capabilities, reducing external dependencies.
*   **Separation of Concerns (Control vs. Core Logic):** The control flow (`k_ausd_v_ta_notice.ksh`) and the core data transformation logic (`d_ausd_v_ta_notice.sql`) were separated into two distinct BigQuery Stored Procedures: `sp_control_ta_notice` and `sp_d_ausd_v_ta_notice`, respectively. This promotes modularity, reusability, and easier maintenance.
*   **BigQuery Native Logging:** Custom shell-based logging and error reporting (`f_alis_msgerr.ksh`, `DWMSG_MeldeFehler`) were replaced by dedicated BigQuery tables (`job_run_log`, `job_error_log`, `job_run_result`). This centralizes logging within BigQuery, making it queryable and integrated with GCP monitoring tools.
*   **Elimination of Temporary Files:** The use of a temporary file (`$DW_DIR_UTL/bert_k_ausd_v_ta_notice_$$.tmp`) for communicating processed record counts was replaced by BigQuery Stored Procedure `OUT` parameters and persistence to the `job_run_result` table. This is a more robust and BigQuery-idiomatic way to pass information between procedures and persist results.
*   **External Orchestration:** The scheduling and invocation of the BigQuery stored procedure are delegated to an external orchestrator (e.g., Cloud Composer/Airflow). This aligns with modern data warehousing practices, allowing for flexible scheduling, dependency management, and monitoring.
*   **`TRUNCATE` and `INSERT` Pattern:** For the `sp_d_ausd_v_ta_notice` procedure, a `TRUNCATE TABLE` followed by an `INSERT INTO` statement is used. This assumes the original `d_ausd_v_ta_notice.sql` performed a full refresh of the target table. This approach simplifies the migration but requires careful consideration if the original logic involved incremental updates or merges.
*   **Error Handling with `EXCEPTION WHEN ERROR`:** BigQuery's `EXCEPTION WHEN ERROR` block is used in `sp_control_ta_notice` to catch and log errors, ensuring that job failures are recorded in `job_error_log` and `job_run_log`, and then re-raised to inform the calling orchestrator.
*   **Parameter Handling:** Shell script parameter parsing was replaced by direct input parameters for the BigQuery stored procedures, which is a native and type-safe mechanism.

## 4. Manual Steps Before Go-Live

Before deploying and running the migrated job, the following manual steps are required:

1.  **BigQuery Project and Dataset Setup:**
    *   Ensure a Google Cloud Project is available.
    *   Create a BigQuery Dataset (e.g., `your_dataset_id`) where all tables and stored procedures will reside. Replace `your_project_id.your_dataset_id` placeholders in the generated code with your actual project and dataset IDs.

2.  **Deploy DDLs:**
    *   Execute the DDL scripts to create the logging and target tables:
        *   `ddl/job_error_log.sql`
        *   `ddl/job_run_log.sql`
        *   `ddl/job_run_result.sql`
        *   `ddl/ta_notice.sql`
    *   Verify that these tables are created successfully in your BigQuery dataset.

3.  **Deploy Stored Procedures:**
    *   Execute the SQL scripts to create the stored procedures:
        *   `sp/sp_d_ausd_v_ta_notice.sql`
        *   `sp/sp_control_ta_notice.sql`
    *   Verify that these procedures are created successfully in your BigQuery dataset.

4.  **Source Table Availability:**
    *   Ensure that the source tables `cds_ta_notice` and `dwtk_meldungen` exist in the same BigQuery dataset (or are accessible via appropriate `project.dataset.table` references) and are populated with relevant data. The `sp_d_ausd_v_ta_notice` procedure specifically references `cds_ta_notice`, and `sp_control_ta_notice` references `dwtk_meldungen`.

5.  **IAM Permissions:**
    *   The service account or user executing the BigQuery job must have the necessary IAM roles:
        *   `BigQuery Data Editor` (or equivalent) on the dataset to create/update tables and stored procedures, and to insert/truncate data.
        *   `BigQuery Job User` to run BigQuery jobs (including stored procedures).
        *   `BigQuery Data Viewer` (or equivalent) on any source tables (`cds_ta_notice`, `dwtk_meldungen`) if they are in a different dataset or project.

6.  **Orchestration Setup:**
    *   Configure your chosen orchestrator (e.g., Cloud Composer/Airflow, Cloud Workflows, Cloud Scheduler with a BigQuery job) to:
        *   Schedule the execution of `CALL your_project_id.your_dataset_id.sp_control_ta_notice(p_job_kennung => 'YOUR_JOB_KENNUNG', p_eintrags_nr => 'YOUR_EINTRAGS_NR');`
        *   Pass the required parameters (`p_job_kennung`, `p_eintrags_nr`). These should align with the values expected by the original job.
        *   Implement retry mechanisms and alerting for job failures.

## 5. Known Gaps & Unresolved References

The following items were identified as risks or require further analysis/follow-up:

*   **Content of `d_ausd_v_ta_notice.sql`:** The migration of `sp_d_ausd_v_ta_notice` was based on a conceptual understanding and a simplified example. The actual, full SQL logic from `d_ausd_v_ta_notice.sql` needs to be thoroughly reviewed, translated to BigQuery SQL, and integrated into `sp_d_ausd_v_ta_notice`. This includes:
    *   Specific SQL constructs and functions.
    *   Table interactions (joins, subqueries).
    *   Any complex business logic or data transformations.
    *   Consideration of BigQuery best practices (partitioning, clustering, data types).
*   **`starteSQLSkript` Behavior:** The exact implementation details of the original `starteSQLSkript` function (e.g., dynamic SQL, specific error handling within the SQL execution, handling of active jobs, transaction management) are not fully known. The current migration assumes a straightforward execution and record count capture. Any complex logic, especially related to job locking or active job checks, needs explicit re-implementation in BigQuery.
*   **Job Deactivation Logic:** The original script's comment "alte aktive Jobs werden einfach dekativiert" (old active jobs are simply deactivated) implies specific logic that is not explicitly migrated in the provided `sp_d_ausd_v_ta_notice` example. This logic needs to be identified from the original `d_ausd_v_ta_notice.sql` and correctly translated into BigQuery SQL within `sp_d_ausd_v_ta_notice`.
*   **Error Handling Fidelity:** While BigQuery's `EXCEPTION WHEN ERROR` and custom logging tables replace the original `f_alis_msgerr.ksh` framework, functional parity in terms of error codes, message formats, and severity levels needs to be verified.
*   **`dwtk_meldungen` Table Schema:** The `sp_control_ta_notice` procedure assumes the existence of a `dwtk_meldungen` table with `timecreated` (TIMESTAMP) and `job_kennung` (STRING) columns for determining `v_process_date`. The actual schema and data in this table need to be confirmed and potentially migrated if it's an Oracle/legacy table.
*   **`cds_ta_notice` Table Schema:** The `sp_d_ausd_v_ta_notice` procedure assumes a `cds_ta_notice` table with columns like `cntrct_id`, `valid_from`, `valid_to`, `entry_date_of_notice`, `insert_at`, `modified_at`, `is_production`. The actual schema and data types of this source table must be verified and mapped correctly to BigQuery. The `FORMAT("%d", n.cntrct_id)` conversion implies `cntrct_id` might be numeric in the source but string in the target.

## 6. Validation

To validate the successful migration and functionality of the BigQuery job, follow these steps:

1.  **Prepare Test Data:**
    *   Populate the `your_project_id.your_dataset_id.cds_ta_notice` table with sample data that covers various scenarios (e.g., new records, updated records, records with different `valid_to` dates, `is_production` flags).
    *   Populate `your_project_id.your_dataset_id.dwtk_meldungen` with a `BERT_DROP_TEMP_TABLE` entry to ensure `v_process_date` is correctly determined.

2.  **Execute the Job:**
    *   Manually execute the `sp_control_ta_notice` stored procedure from the BigQuery console or via your orchestrator with test parameters:
        ```sql
        CALL your_project_id.your_dataset_id.sp_control_ta_notice(
            p_job_kennung => 'TEST_JOB_KENNUNG',
            p_eintrags_nr => 'TEST_EINTRAGS_NR'
        );
        ```

3.  **Verify "Passing" Criteria:**

    *   **Successful Execution:**
        *   Query `your_project_id.your_dataset_id.job_run_log`:
            ```sql
            SELECT * FROM your_project_id.your_dataset_id.job_run_log
            WHERE job_kennung = 'TEST_JOB_KENNUNG' AND eintrags_nr = 'TEST_EINTRAGS_NR'
            ORDER BY created_ts DESC;
            ```
            Expected result: Two entries, one with `status = 'STARTED'` and one with `status = 'COMPLETED'`.
        *   Query `your_project_id.your_dataset_id.job_run_result`:
            ```sql
            SELECT * FROM your_project_id.your_dataset_id.job_run_result
            WHERE job_kennung = 'TEST_JOB_KENNUNG' AND eintrags_nr = 'TEST_EINTRAGS_NR';
            ```
            Expected result: One entry with `record_count` matching the number of rows inserted into `ta_notice`.
        *   Query `your_project_id.your_dataset_id.ta_notice`:
            ```sql
            SELECT * FROM your_project_id.your_dataset_id.ta_notice;
            ```
            Expected result: The `ta_notice` table should contain the data processed from `cds_ta_notice` according to the logic in `sp_d_ausd_v_ta_notice`. Verify data correctness and completeness against the source.
        *   Query `your_project_id.your_dataset_id.job_error_log`:
            ```sql
            SELECT * FROM your_project_id.your_dataset_id.job_error_log
            WHERE job_kennung = 'TEST_JOB_KENNUNG' AND eintrags_nr = 'TEST_EINTRAGS_NR';
            ```
            Expected result: No entries for a successful run.

    *   **Error Handling (Negative Testing):**
        *   Execute `sp_control_ta_notice` with `NULL` or empty `p_job_kennung` or `p_eintrags_nr`:
            ```sql
            CALL your_project_id.your_dataset_id.sp_control_ta_notice(
                p_job_kennung => NULL,
                p_eintrags_nr => 'TEST_EINTRAGS_NR_ERROR'
            );
            ```
            Expected result: The call should fail with an error message indicating invalid parameters.
        *   Query `your_project_id.your_dataset_id.job_run_log`:
            Expected result: An entry with `status = 'STARTED'` and another with `status = 'FAILED'`.
        *   Query `your_project_id.your_dataset_id.job_error_log`:
            Expected result: An entry detailing the parameter validation error.
        *   (Optional) Simulate an error within `sp_d_ausd_v_ta_notice` (e.g., by trying to insert invalid data or referencing a non-existent table) and verify that `sp_control_ta_notice` catches and logs it correctly.

## 7. Rollback Procedure

In case of critical issues or unexpected behavior after go-live, the following rollback procedure can be executed:

1.  **Deactivate New Orchestration:**
    *   Immediately pause or disable the new BigQuery job orchestration (e.g., Cloud Composer DAG, Cloud Scheduler job) that invokes `sp_control_ta_notice`. This stops any further execution of the migrated job.

2.  **Reactivate Original Job:**
    *   Re-enable the scheduling or invocation mechanism for the original KornShell script (`k_ausd_v_ta_notice.ksh`). Ensure it points to the original source systems and configurations.

3.  **Data Reversion (if necessary):**
    *   **For `ta_notice`:** Since `sp_d_ausd_v_ta_notice` uses a `TRUNCATE` and `INSERT` pattern, if the data in `your_project_id.your_dataset_id.ta_notice` is incorrect, it can be truncated or deleted. The next successful run of the original job (if it targets a similar BigQuery table or a different system) would then repopulate the correct data. If the original job also targets `ta_notice` (unlikely if this is a migration), then the data would be overwritten by the original job.
    *   **For Logging Tables (`job_run_log`, `job_error_log`, `job_run_result`):** These tables are append-only. No specific rollback is needed for them, as they simply record events. The new entries will remain but will be clearly identifiable as belonging to the migrated job's execution.

4.  **Investigation and Remediation:**
    *   Analyze the `job_error_log` and `job_run_log` tables in BigQuery for insights into the failure.
    *   Review BigQuery job history and logs for detailed error messages and performance issues.
    *   Address the root cause of the problem in the BigQuery stored procedures or orchestration.

5.  **Re-deployment (after fix):**
    *   Once the issues are resolved, follow the "Manual Steps Before Go-Live" and "Validation" procedures again to ensure the fix is effective before attempting another go-live.