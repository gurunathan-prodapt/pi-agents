# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell wrapper script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vertrag_tmp.ksh`. This script, responsible for orchestrating the "Vertragsdatenabgleich" (contract data reconciliation) process and managing logging/error handling for the `ta_vertrag_tmp` table, has been migrated to Google BigQuery.

The migration transforms the shell script into a BigQuery Stored Procedure, `my_gcp_project.dw_isrpt_isbert_prod.sp_vertragsdatenabgleich`, which now handles parameter parsing, environment setup, and robust error trapping within the BigQuery environment. It also integrates with a new BigQuery-native logging and status tracking system, replacing the original file-based approach. The core business logic, originally in `k_ausd_v_ta_vertrag_tmp.ksh`, is now represented by a placeholder BigQuery Stored Procedure, `my_gcp_project.dw_isrpt_isbert_prod.sp_k_ausd_v_ta_vertrag_tmp`, which will be fully implemented in a subsequent phase.

## 2. Generated artifacts

The migration process generated the following BigQuery SQL files:

*   **`my_gcp_project.dw_isrpt_isbert_prod/create_dataset.sql`**
    *   **Role:** Creates the target BigQuery dataset `my_gcp_project.dw_isrpt_isbert_prod`. This dataset will house all migrated tables and stored procedures related to the `isrpt.isbert` domain.
*   **`my_gcp_project.dw_isrpt_isbert_prod/create_ta_vertrag_tmp_table.sql`**
    *   **Role:** Defines the schema for the `ta_vertrag_tmp` table in BigQuery. This table is the primary data target for the contract data reconciliation process. The schema provided is a placeholder and will be refined during the migration of the core logic.
*   **`my_gcp_project.dw_isrpt_isbert_prod/create_job_registry_table.sql`**
    *   **Role:** Creates the `job_registry` table. This table serves as a central repository for metadata about each job execution, including start/end times, overall status, and key parameters. It replaces parts of the original file-based logging.
*   **`my_gcp_project.dw_isrpt_isbert_prod/create_job_audit_log_table.sql`**
    *   **Role:** Creates the `job_audit_log` table. This table stores detailed, timestamped log messages for each job run, including informational messages, warnings, and errors. It replaces the detailed log file output of the original script.
*   **`my_gcp_project.dw_isrpt_isbert_prod/create_job_status_table.sql`**
    *   **Role:** Creates the `job_status` table. This table tracks the current and most recent status of each job execution, providing a quick overview of job health. It complements the `job_registry` and `job_audit_log` tables.
*   **`my_gcp_project.dw_isrpt_isbert_prod/sp_k_ausd_v_ta_vertrag_tmp.sql`**
    *   **Role:** This is a placeholder BigQuery Stored Procedure for the core business logic originally contained in `k_ausd_v_ta_vertrag_tmp.ksh`. It includes basic logging for start/end and error handling, but the actual data transformation and reconciliation logic is yet to be migrated and implemented here.
*   **`my_gcp_project.dw_isrpt_isbert_prod/sp_vertragsdatenabgleich.sql`**
    *   **Role:** This is the main migrated BigQuery Stored Procedure, replacing `r_ausd_v_ta_vertrag_tmp.ksh`. It acts as the wrapper, handling input parameters, initializing job metadata, orchestrating the call to `sp_k_ausd_v_ta_vertrag_tmp`, and managing comprehensive logging and error handling using the new BigQuery logging tables.

## 3. Key design decisions

The migration strategy for `r_ausd_v_ta_vertrag_tmp.ksh` to BigQuery was driven by the following key design decisions:

*   **Wrapper to BigQuery Stored Procedure:** The KornShell wrapper script was directly translated into a BigQuery Stored Procedure (`sp_vertragsdatenabgleich`). This allows for native execution within BigQuery, leveraging its robust SQL capabilities for orchestration, parameter handling, and error management.
*   **Core Logic as Separate Stored Procedure:** The core processing script (`k_ausd_v_ta_vertrag_tmp.ksh`) is designated to be migrated into its own BigQuery Stored Procedure (`sp_k_ausd_v_ta_vertrag_tmp`). This promotes modularity, reusability, and clear separation of concerns between orchestration and business logic.
*   **BigQuery-Native Logging and Status:** The original file-based logging and status tracking (`LogDatei`, `DWMSG_...` functions) have been replaced by dedicated BigQuery tables (`job_registry`, `job_audit_log`, `job_status`). This centralizes logging, enables easier querying and monitoring, and aligns with cloud-native best practices.
*   **Parameter and Environment Variable Translation:** Shell script parameters (`getopts`) and environment variables (`ProgName`, `JobKennung`, `BERT_DIR_ROOT`) are mapped to BigQuery Stored Procedure `IN` parameters and `DECLARE` variables, respectively. This ensures all necessary context is available within the BigQuery execution environment.
*   **Utility Script Replacement:** Sourced shell utility scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) are replaced by native BigQuery SQL constructs, helper procedures (e.g., `LogAudit`, `UpdateJobStatus`), or direct embedding of their logic where appropriate.
*   **BigQuery `EXCEPTION` for Error Handling:** The KornShell `trap` mechanism for error handling is replaced by BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks. This provides structured error capture and allows for consistent logging of failures to the `job_audit_log` and `job_status` tables.
*   **Trade-offs:**
    *   **Placeholder Core Logic:** The most significant trade-off is that the detailed transformation logic of `k_ausd_v_ta_vertrag_tmp.ksh` is currently a placeholder. This defers the full implementation of the reconciliation process, requiring a subsequent design and development phase.
    *   **"DWMSG" Functionality:** The migration assumes `DWMSG_...` functions are primarily for logging and status updates. If they had complex side effects (e.g., external system calls not evident in the wrapper), these would need further investigation.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps are required:

1.  **BigQuery Dataset Creation:**
    *   Ensure the `my_gcp_project.dw_isrpt_isbert_prod` BigQuery dataset is created. This can be done by executing `my_gcp_project.dw_isrpt_isbert_prod/create_dataset.sql` or manually via the GCP Console.
2.  **Table Creation:**
    *   Execute the DDL scripts for the logging and data tables:
        *   `my_gcp_project.dw_isrpt_isbert_prod/create_ta_vertrag_tmp_table.sql`
        *   `my_gcp_project.dw_isrpt_isbert_prod/create_job_registry_table.sql`
        *   `my_gcp_project.dw_isrpt_isbert_prod/create_job_audit_log_table.sql`
        *   `my_gcp_project.dw_isrpt_isbert_prod/create_job_status_table.sql`
3.  **Stored Procedure Deployment:**
    *   Deploy the generated BigQuery Stored Procedures:
        *   `my_gcp_project.dw_isrpt_isbert_prod/sp_k_ausd_v_ta_vertrag_tmp.sql` (placeholder)
        *   `my_gcp_project.dw_isrpt_isbert_prod/sp_vertragsdatenabgleich.sql`
4.  **IAM/Permissions:**
    *   Grant the service account or user executing the BigQuery stored procedure the necessary IAM roles:
        *   `BigQuery Data Editor` on `my_gcp_project.dw_isrpt_isbert_prod` (to insert into log tables and modify `ta_vertrag_tmp`).
        *   `BigQuery Job User` (to run BigQuery jobs, including stored procedures).
        *   If `ta_vertrag_tmp` interacts with other datasets, ensure appropriate `BigQuery Data Viewer`/`Editor` roles are granted there as well.
5.  **Initial Data Load for `ta_vertrag_tmp`:**
    *   If `ta_vertrag_tmp` is not empty in the source system, an initial data load must be performed to populate `my_gcp_project.dw_isrpt_isbert_prod.ta_vertrag_tmp`. This might involve using BigQuery Data Transfer Service, `bq load` commands, or Dataflow jobs.
6.  **Scheduling Configuration:**
    *   Update the existing job scheduler (e.g., Cloud Composer/Airflow, Cloud Workflows, or other orchestration tools) to invoke the new BigQuery Stored Procedure:
        `CALL my_gcp_project.dw_isrpt_isbert_prod.sp_vertragsdatenabgleich(p_job_kennung => '...', p_run_date => '...', ...);`
    *   Ensure the scheduler is configured with the correct service account and permissions.
7.  **Secrets Management (if applicable):**
    *   While the wrapper script itself doesn't expose explicit secrets, if the core logic (once implemented) requires access to external systems or sensitive credentials, these must be securely managed using Google Secret Manager and accessed appropriately within the BigQuery environment or via external processing (e.g., Dataflow).

## 5. Known gaps & unresolved references

The following items are identified as known gaps or require further follow-up:

*   **Core Script Logic (`sp_k_ausd_v_ta_vertrag_tmp`):** The most significant gap is that `my_gcp_project.dw_isrpt_isbert_prod.sp_k_ausd_v_ta_vertrag_tmp` is currently a placeholder. The detailed business logic for contract data reconciliation, originally in `k_ausd_v_ta_vertrag_tmp.ksh`, needs to be fully designed, migrated, and implemented. This will involve a separate analysis of the source script's SQL and shell commands to determine the optimal BigQuery implementation (e.g., BigQuery SQL, Dataflow, PySpark).
*   **"DWMSG" Functionality Beyond Logging:** The migration assumes the `DWMSG_...` functions primarily handle logging and status updates. If any of these functions had complex side effects, such as triggering external alerts via non-standard channels or interacting with other systems in ways not evident from the wrapper script, these functionalities would need further investigation and specific migration plans.
*   **Input Parameters (`-s`, `-l`) Usage:** The original wrapper script parsed `-s` and `-l` parameters but did not explicitly process them, implying they were passed to the core script. The exact usage and expected values of `p_s_param` and `p_l_param` within the core logic (`sp_k_ausd_v_ta_vertrag_tmp`) need to be confirmed during the core script's migration to ensure proper parameter propagation and handling.
*   **`ta_vertrag_tmp` Schema Refinement:** The provided schema for `my_gcp_project.dw_isrpt_isbert_prod.ta_vertrag_tmp` is a generic placeholder. A detailed analysis of the source `ta_vertrag_tmp` table and the `k_ausd_v_ta_vertrag_tmp.ksh` script is required to define the precise and complete BigQuery schema, including data types, partitioning, clustering, and any specific column descriptions.
*   **Complexity Tier Validation:** The complexity tier for the original script was assumed to be "Medium." A thorough manual review of the `k_ausd_v_ta_vertrag_tmp.ksh` script and any other related components is necessary to validate this assumption. If the actual complexity is higher, the effort estimate and migration strategy for the core logic may need adjustment.

## 6. Validation

To validate the successful migration and functionality of the `sp_vertragsdatenabgleich` stored procedure, follow these steps:

1.  **Execute the Stored Procedure:**
    *   **Successful Run:**
        ```sql
        CALL `my_gcp_project.dw_isrpt_isbert_prod.sp_vertragsdatenabgleich`(
            p_job_kennung => 'TEST_VERTRAG_OK',
            p_run_date => CURRENT_DATE()
        );
        ```
    *   **Run with Parameters:**
        ```sql
        CALL `my_gcp_project.dw_isrpt_isbert_prod.sp_vertragsdatenabgleich`(
            p_job_kennung => 'TEST_VERTRAG_PARAMS',
            p_run_date => '2023-01-15',
            p_s_param => 'some_s_value',
            p_l_param => 'some_l_value'
        );
        ```
    *   **Help Message:**
        ```sql
        CALL `my_gcp_project.dw_isrpt_isbert_prod.sp_vertragsdatenabgleich`(p_show_help => TRUE);
        ```
    *   **Simulated Error Run (once core SP is implemented with error conditions):**
        *   This will require modifying `sp_k_ausd_v_ta_vertrag_tmp` to intentionally raise an error for testing purposes.
        ```sql
        -- Example: CALL `my_gcp_project.dw_isrpt_isbert_prod.sp_vertragsdatenabgleich`(p_job_kennung => 'TEST_VERTRAG_ERR', p_run_date => CURRENT_DATE(), p_simulate_error => TRUE);
        ```

2.  **Check Logging and Status Tables:**
    *   **`job_registry`:**
        ```sql
        SELECT * FROM `my_gcp_project.dw_isrpt_isbert_prod.job_registry` ORDER BY start_time DESC LIMIT 5;
        ```
        *   **Passing Criteria:** For successful runs, `status` should be 'OK', `end_time` should be populated, and `error_code`/`error_message` should be NULL. For error runs, `status` should be 'ERR', and `error_code`/`error_message` should contain relevant details.
    *   **`job_status`:**
        ```sql
        SELECT * FROM `my_gcp_project.dw_isrpt_isbert_prod.job_status` ORDER BY status_timestamp DESC LIMIT 5;
        ```
        *   **Passing Criteria:** `current_status` should reflect the final state ('OK' or 'ERR'), and `last_update_message` should correspond to the job's outcome.
    *   **`job_audit_log`:**
        ```sql
        SELECT * FROM `my_gcp_project.dw_isrpt_isbert_prod.job_audit_log` WHERE job_run_id = (SELECT job_run_id FROM `my_gcp_project.dw_isrpt_isbert_prod.job_registry` ORDER BY start_time DESC LIMIT 1) ORDER BY log_timestamp;
        ```
        *   **Passing Criteria:**
            *   For successful runs, expect a sequence of 'INFO' messages, including job start, parameter logging, core script start/end, and the final "Die Abarbeitung wurde ohne erkennbare Fehler beendet" message. No 'ERROR' messages should be present.
            *   For error runs, expect 'ERROR' messages detailing the failure, along with preceding 'INFO' messages up to the point of failure.
            *   Verify that parameters (`p_s_param`, `p_l_param`) are correctly logged.

3.  **Verify Help Message:**
    *   When `p_show_help` is TRUE, the output should be a single row containing the formatted help text, and no entries should be made in the logging tables.

4.  **Data Verification (once core SP is implemented):**
    *   After `sp_k_ausd_v_ta_vertrag_tmp` is fully implemented, verify that `my_gcp_project.dw_isrpt_isbert_prod.ta_vertrag_tmp` is updated correctly according to the expected reconciliation logic.

## 7. Rollback procedure

In case of issues or a decision to revert the migration, follow these steps:

1.  **Stop New Invocations:**
    *   Immediately disable or revert the scheduler configuration that invokes `my_gcp_project.dw_isrpt_isbert_prod.sp_vertragsdatenabgleich`.
    *   Re-enable the original scheduler configuration that invokes `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vertrag_tmp.ksh`.
2.  **Monitor Original Job:**
    *   Verify that the original KornShell script is executing correctly and producing expected outputs.
3.  **Data Rollback (if `ta_vertrag_tmp` was modified):**
    *   If the `sp_k_ausd_v_ta_vertrag_tmp` (even in its placeholder state, if it performed any DML) or the fully implemented core logic modified `my_gcp_project.dw_isrpt_isbert_prod.ta_vertrag_tmp`, and these changes are deemed incorrect or undesirable, restore the `ta_vertrag_tmp` table from a known good backup taken *before* the migration go-live.
    *   Alternatively, if the changes are minor and reversible, execute specific DML statements to revert the data.
4.  **Clean Up BigQuery Objects (Optional but Recommended):**
    *   Once the rollback is confirmed successful and the original job is stable, you may choose to delete the migrated BigQuery objects to avoid confusion and costs:
        *   Drop the stored procedures:
            ```sql
            DROP PROCEDURE IF EXISTS `my_gcp_project.dw_isrpt_isbert_prod.sp_vertragsdatenabgleich`;
            DROP PROCEDURE IF EXISTS `my_gcp_project.dw_isrpt_isbert_prod.sp_k_ausd_v_ta_vertrag_tmp`;
            ```
        *   Drop the tables (ensure no critical data is lost if `ta_vertrag_tmp` was used):
            ```sql
            DROP TABLE IF EXISTS `my_gcp_project.dw_isrpt_isbert_prod.job_status`;
            DROP TABLE IF EXISTS `my_gcp_project.dw_isrpt_isbert_prod.job_audit_log`;
            DROP TABLE IF EXISTS `my_gcp_project.dw_isrpt_isbert_prod.job_registry`;
            DROP TABLE IF EXISTS `my_gcp_project.dw_isrpt_isbert_prod.ta_vertrag_tmp`;
            ```
        *   Drop the dataset (only if no other migrated objects reside there):
            ```sql
            DROP SCHEMA IF EXISTS `my_gcp_project.dw_isrpt_isbert_prod`;
            ```
5.  **Review and Redesign:**
    *   Analyze the reasons for the rollback and update the migration design document to address any identified issues before attempting re-migration.