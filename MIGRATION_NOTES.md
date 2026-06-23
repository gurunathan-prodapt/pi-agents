# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `k_ausd_v_ta_vvl_upgrade.ksh` from its on-premise environment to Google Cloud Platform (GCP). The original script served as an orchestration wrapper for a SQL script (`d_ausd_v_ta_vvl_upgrade.sql`), handling parameter parsing, error handling, and result logging.

The migration targets BigQuery for data processing and orchestration. The KornShell script's control logic has been re-implemented as a BigQuery stored procedure, `r_ausd_vertrag_control`, which in turn calls a separate BigQuery stored procedure, `d_ausd_v_ta_vvl_upgrade_proc`, containing the migrated core SQL logic. Custom shell-based logging and temporary file mechanisms have been replaced with dedicated BigQuery logging tables.

## 2. Generated artifacts

The migration process generated the following BigQuery DDL and stored procedure definitions:

*   **`ddl/ta_vvl_upgrade.sql`**
    *   **Role:** Defines the schema for the target data table `project.dataset.ta_vvl_upgrade` in BigQuery. This table will store the final processed data, replacing the original `ta_vvl_upgrade` table.
*   **`ddl/job_table.sql`**
    *   **Role:** Defines the schema for the `project.dataset.job_table` logging table. This table tracks the status and metadata of each job run, replacing the implicit job tracking of the original shell script.
*   **`ddl/job_error_log.sql`**
    *   **Role:** Defines the schema for the `project.dataset.job_error_log` table. This table captures detailed error messages and context, replacing the custom shell error handling framework (`f_alis_msgerr.ksh`).
*   **`ddl/job_result_log.sql`**
    *   **Role:** Defines the schema for the `project.dataset.job_result_log` table. This table stores the number of records processed by the core SQL logic and job completion details, replacing the temporary file mechanism (`$DW_DIR_UTL/bert_k_ausd_v_ta_vvl_upgrade_$$.tmp`) used to capture record counts.
*   **`stored_procedures/d_ausd_v_ta_vvl_upgrade_proc.sql`**
    *   **Role:** This BigQuery stored procedure (`project.dataset.d_ausd_v_ta_vvl_upgrade_proc`) is a placeholder for the migrated core SQL logic from `d_ausd_v_ta_vvl_upgrade.sql`. It is designed to perform the data transformation and updates on `ta_vvl_upgrade` and return the count of processed records. **Note: The actual Oracle SQL content needs to be manually converted to BigQuery SQL and inserted into this procedure.**
*   **`stored_procedures/r_ausd_vertrag_control.sql`**
    *   **Role:** This BigQuery stored procedure (`project.dataset.r_ausd_vertrag_control`) is the main orchestration component. It replaces the `k_ausd_v_ta_vvl_upgrade.ksh` script, handling parameter validation, job registration, calling `d_ausd_v_ta_vvl_upgrade_proc`, and logging job results and errors to the respective BigQuery tables.

## 3. Key design decisions

*   **BigQuery Stored Procedures for Orchestration:** The KornShell script's orchestration logic (parameter parsing, validation, error handling, script execution) was re-implemented as a BigQuery stored procedure (`r_ausd_vertrag_control`). This centralizes the logic within the data platform, leveraging BigQuery's native capabilities for scripting, error handling, and logging, and eliminating the need for external shell environments.
*   **Separation of Concerns (Control vs. Core Logic):** The orchestration logic (`r_ausd_vertrag_control`) and the core data transformation logic (`d_ausd_v_ta_vvl_upgrade_proc`) are implemented as distinct BigQuery stored procedures. This promotes modularity, reusability, and easier maintenance, allowing the core SQL logic to be developed and tested independently.
*   **Dedicated BigQuery Logging Tables:** The custom shell-based error framework (`f_alis_msgerr.ksh`) and temporary file-based result capture were replaced by structured BigQuery tables (`job_table`, `job_error_log`, `job_result_log`). This provides a standardized, queryable, and persistent logging mechanism, improving observability and debugging capabilities.
*   **Parameter Handling via Stored Procedure Arguments:** Command-line arguments (`JobKennung`, `EintragsNr`) are directly mapped to input parameters of the BigQuery stored procedure. This simplifies invocation and type safety compared to shell script parsing.
*   **Trade-offs:**
    *   **Loss of Direct Shell Environment Control:** The migration moves away from a shell-centric environment, meaning any specific shell utilities or environment variable dependencies not explicitly re-implemented in BigQuery SQL are no longer available.
    *   **BigQuery SQL Dialect Conversion:** The core SQL logic from `d_ausd_v_ta_vvl_upgrade.sql` (likely Oracle PL/SQL) requires careful manual conversion to BigQuery SQL, which can be a significant effort depending on complexity and specific functions used.
    *   **New Orchestration Layer:** While the BigQuery stored procedure handles internal orchestration, if the original job was part of a larger scheduled workflow (e.g., UC4), an external GCP orchestration tool (like Cloud Composer/Airflow or Cloud Scheduler) will be required to invoke the BigQuery stored procedure.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **GCP Project and Dataset Setup:**
    *   Ensure the target GCP project (`project`) and BigQuery dataset (`dataset`) exist. If not, create them.
2.  **IAM Permissions:**
    *   Grant appropriate BigQuery IAM roles to the service account or user that will execute the stored procedures. This typically includes `BigQuery Data Editor` for the dataset containing the tables and procedures, and `BigQuery Job User` for running queries and procedures.
3.  **Deploy DDLs:**
    *   Execute the DDL scripts (`ddl/ta_vvl_upgrade.sql`, `ddl/job_table.sql`, `ddl/job_error_log.sql`, `ddl/job_result_log.sql`) in the target BigQuery dataset to create the necessary tables.
4.  **Migrate Core SQL Logic:**
    *   **Crucially, the content of `d_ausd_v_ta_vvl_upgrade.sql` must be manually converted from its original SQL dialect (likely Oracle) to BigQuery SQL.** This converted logic must then be inserted into the `stored_procedures/d_ausd_v_ta_vvl_upgrade_proc.sql` file, replacing the placeholder comments. Ensure the procedure correctly returns the `processed_records` count.
5.  **Deploy Stored Procedures:**
    *   Execute the `stored_procedures/d_ausd_v_ta_vvl_upgrade_proc.sql` and `stored_procedures/r_ausd_vertrag_control.sql` scripts in the target BigQuery dataset to create the stored procedures.
6.  **Data Ingestion (if applicable):**
    *   Ensure that any source tables referenced by the migrated `d_ausd_v_ta_vvl_upgrade_proc` (e.g., `sof$ta_vvl_dwh`, `dwh$ta_l_bindefr_aendgr_carm`) are already migrated and populated in BigQuery with the necessary data.
7.  **Scheduling Configuration:**
    *   If the original job was scheduled, configure a new scheduler in GCP (e.g., Cloud Composer/Airflow DAG, Cloud Scheduler job calling a Cloud Function/Workflows) to invoke the `project.dataset.r_ausd_vertrag_control` stored procedure with the required `p_JobKennung` and `p_EintragsNr` parameters.

## 5. Known gaps & unresolved references

*   **`d_ausd_v_ta_vvl_upgrade.sql` Migration:** The most significant gap is the manual migration of the core business logic within `d_ausd_v_ta_vvl_upgrade.sql` from its original SQL dialect (likely Oracle PL/SQL) to BigQuery SQL. This is a prerequisite for the entire job to function correctly. The provided `d_ausd_v_ta_vvl_upgrade_proc.sql` is a placeholder.
*   **Implicit Dependencies:** The original script sourced several utility KornShell scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`). While their functionalities have been conceptually replaced (e.g., logging to `job_error_log`, BigQuery date functions, SP parameters), a thorough review of their exact functionalities is needed to ensure no subtle logic was missed.
*   **Custom Error Framework Details:** The original `f_alis_msgerr.ksh` likely had specific error codes and messages. The BigQuery `job_error_log` captures generic error messages. A detailed mapping of original error codes/messages to BigQuery equivalents or custom codes might be needed for consistency.
*   **Parameter Mapping Confirmation:** The exact data types and expected values for `p_JobKennung` and `p_EintragsNr` in BigQuery should be confirmed based on their usage in the fully migrated `d_ausd_v_ta_vvl_upgrade.sql` logic.
*   **"Deactivate Old Active Jobs" Logic:** The comment in the original design document about "alte aktive Jobs werden einfach dekativiert" (old active jobs are simply deactivated) needs to be explicitly identified within the `d_ausd_v_ta_vvl_upgrade.sql` logic or related components and ensured it is correctly translated and implemented in the BigQuery solution.
*   **Source Table Schemas:** The DDL for `ta_vvl_upgrade` is inferred. The schemas for source tables like `sof$ta_vvl_dwh` and `dwh$ta_l_bindefr_aendgr_carm` (referenced in the original SQL) must be accurately defined in BigQuery for `d_ausd_v_ta_vvl_upgrade_proc` to work.

## 6. Validation

To validate the migrated job, follow these steps:

1.  **Prepare Test Data:**
    *   Populate the necessary source tables in BigQuery (e.g., `project.dataset.sof_ta_vvl_dwh`, `project.dataset.dwh_ta_l_bindefr_aendgr_carm`) with representative test data that mimics the production environment. Include edge cases and data that would trigger both successful and error scenarios.
2.  **Execute the Main Stored Procedure:**
    *   Invoke the `r_ausd_vertrag_control` stored procedure with sample parameters:
        ```sql
        CALL `project.dataset.r_ausd_vertrag_control`('TEST_JOB_KENNUNG_1', 'TEST_ENTRY_NR_1');
        ```
    *   Test with valid parameters, invalid/missing parameters (to check validation logic), and scenarios that would cause errors in the core SQL logic.
3.  **Verify Logging Tables:**
    *   **`project.dataset.job_table`**: Query this table to ensure a new entry was created for the job run, with the correct `job_kennung`, `eintrags_nr`, `tab_name`, and `status` (e.g., 'RUNNING', 'COMPLETED', 'FAILED'). Check `created_ts` and `updated_ts`.
    *   **`project.dataset.job_result_log`**: For successful runs, query this table to verify that an entry exists with the correct `job_kennung`, `eintrags_nr`, `tab_name`, and `records_processed`. The `records_processed` count should match the expected number of rows affected by the `d_ausd_v_ta_vvl_upgrade_proc`.
    *   **`project.dataset.job_error_log`**: For failed runs, query this table to confirm that an error entry was logged with the correct `error_ts`, `procedure_name`, `error_code`, `error_message`, `job_kennung`, and `eintrags_nr`.
4.  **Verify Target Data:**
    *   Query `project.dataset.ta_vvl_upgrade` to ensure that the data has been correctly processed and inserted/updated as expected by the `d_ausd_v_ta_vvl_upgrade_proc`. Compare the results with the expected output based on the original script's behavior.
5.  **"Passing" Criteria:**
    *   **Successful Execution:**
        *   `job_table` shows `status = 'COMPLETED'`.
        *   `job_result_log` contains an entry with the correct `records_processed` count.
        *   `job_error_log` contains no entries for the specific job run.
        *   `ta_vvl_upgrade` contains the correct, transformed data.
    *   **Error Handling:**
        *   `job_table` shows `status = 'FAILED'`.
        *   `job_error_log` contains an entry with an appropriate `error_code` and `error_message`.
        *   The procedure terminates gracefully without unhandled exceptions.

## 7. Rollback procedure

In case of issues or unexpected behavior after go-live, the rollback procedure involves reverting to the original KornShell script and its associated environment.

1.  **Disable New BigQuery Job:**
    *   If scheduled via Cloud Composer/Airflow, disable the DAG.
    *   If scheduled via Cloud Scheduler, disable or delete the job.
    *   Ensure no further invocations of `project.dataset.r_ausd_vertrag_control` occur.
2.  **Re-enable Original Job:**
    *   Re-enable the original scheduling mechanism for `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_vvl_upgrade.ksh` (e.g., UC4 job).
    *   Verify that the original script can connect to its source database and execute successfully.
3.  **Data State (Optional, if critical):**
    *   If the BigQuery job made irreversible changes to `project.dataset.ta_vvl_upgrade` that conflict with the original system, a data rollback or reconciliation might be necessary. This would involve restoring `ta_vvl_upgrade` to a previous state (e.g., using BigQuery time travel or a backup) or running a corrective script. This step is highly dependent on the specific impact of the failed BigQuery job.
4.  **Monitor Original Job:**
    *   Closely monitor the re-enabled original job to ensure it functions as expected and processes data correctly.
5.  **Analyze and Rectify:**
    *   Investigate the cause of the failure in the BigQuery migration using the `job_error_log` and BigQuery logs. Rectify the issues in the BigQuery DDLs or stored procedures before attempting another deployment.