# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_cntrct_dist.ksh`. This script, originally responsible for orchestrating the preparation of contract cache data for the BERT system, including parameter handling, date determination, error management, and invoking core business logic, has been re-engineered.

The migration target is Google BigQuery, where the script's functionality is now implemented using BigQuery Stored Procedures. The original UC4 scheduling will be replaced by a modern cloud orchestrator, such as Cloud Composer (Apache Airflow).

## 2. Generated artifacts

The migration process generated the following BigQuery SQL files and conceptual orchestrator configuration:

*   **`project/dataset/job_log.sql`**
    *   **Role:** Defines the Data Definition Language (DDL) for the `job_log` table in BigQuery. This table serves as a centralized, structured repository for all log messages generated during the execution of the migrated job, replacing the original file-based logging.
*   **`project/dataset/job_status.sql`**
    *   **Role:** Defines the DDL for the `job_status` table in BigQuery. This table tracks the overall execution status (e.g., 'OK', 'FAILED') of the job, providing a quick overview of recent runs.
*   **`project/dataset/ausd_bp_ta_cntrct_dist_core.sql`**
    *   **Role:** Contains the BigQuery Stored Procedure `ausd_bp_ta_cntrct_dist_core`. This procedure encapsulates the core business logic, including data deletion and insertion operations on the target `fos_tabelle` based on the source `dwh_vertrag_cache` and the provided parameters (stichtag, restart value). It replaces the functionality of the original `k_ausd_bp_ta_cntrct_dist.ksh` script.
*   **`project/dataset/ausd_bp_ta_cntrct_dist_wrapper.sql`**
    *   **Role:** Contains the BigQuery Stored Procedure `ausd_bp_ta_cntrct_dist_wrapper`. This procedure acts as the main entry point and orchestrator, replacing the original `r_ausd_bp_ta_cntrct_dist.ksh`. It handles parameter validation, date derivation, logging to `job_log`, error handling, and invokes the `ausd_bp_ta_cntrct_dist_core` procedure.
*   **`[Orchestrator_DAG_File]` (e.g., `ausd_bp_ta_cntrct_dist_dag.py` for Airflow)**
    *   **Role:** (Conceptual) This file represents the configuration for the chosen cloud orchestrator (e.g., Cloud Composer/Apache Airflow). It defines the schedule, parameters, and task to invoke the `project.dataset.ausd_bp_ta_cntrct_dist_wrapper` BigQuery Stored Procedure. It replaces the original UC4 scheduling mechanism.

## 3. Key design decisions

The following key design decisions were made during the migration:

*   **BigQuery Stored Procedures for Logic Encapsulation**: The entire KornShell script's orchestration and core business logic were re-engineered into BigQuery Stored Procedures (`_wrapper` and `_core`). This approach leverages BigQuery's native capabilities for data manipulation, reduces context switching between different technologies, and keeps the processing logic close to the data, improving performance and maintainability.
*   **Structured Logging and Status Tracking**: Instead of file-based logging and ad-hoc status updates, dedicated BigQuery tables (`job_log` and `job_status`) were introduced. This provides centralized, queryable, and persistent logging and job status tracking, facilitating easier monitoring, auditing, and debugging.
*   **Native BigQuery Parameter Handling and Date Functions**: The shell script's parameter parsing (`getopts`) and date derivation logic were replaced by BigQuery's `IN` parameters, `IFNULL`, `NULLIF`, `CURRENT_DATE()`, and `FORMAT_DATE()` functions. This aligns with standard SQL practices and integrates seamlessly with BigQuery's execution environment.
*   **Robust Error Handling within BigQuery**: The `set -e` and `trap` mechanisms of KornShell were replaced by BigQuery's `BEGIN ... EXCEPTION WHEN ERROR THEN ... END` blocks. This provides a structured and robust way to catch and handle errors within the database, ensuring proper logging and status updates even in case of failures.
*   **Modern Cloud Orchestration**: The dependency on the legacy UC4 scheduler was replaced by a modern cloud orchestrator (e.g., Cloud Composer). This enables cloud-native scheduling, monitoring, and integration with other GCP services, offering greater scalability, reliability, and operational efficiency.
*   **Separation of Concerns**: The original script's implicit separation between orchestration (wrapper) and core business logic was explicitly maintained and formalized by creating two distinct BigQuery Stored Procedures: `ausd_bp_ta_cntrct_dist_wrapper` for orchestration and `ausd_bp_ta_cntrct_dist_core` for data processing. This promotes modularity, reusability, and easier maintenance.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**:
    *   Ensure the target BigQuery dataset (`project.dataset`) exists. If not, create it in the Google Cloud Console or via `bq mk --dataset project:dataset`.
2.  **Source and Target Table Existence**:
    *   Verify that the source table (`project.dataset.dwh_vertrag_cache`) and the target table (`project.dataset.fos_tabelle`) exist in the specified BigQuery dataset and have the correct schema definitions. If they do not exist, they must be created.
3.  **IAM Permissions Configuration**:
    *   **Service Account for Orchestrator**: The service account used by the Cloud Composer environment (or other orchestrator) must have the necessary BigQuery permissions:
        *   `bigquery.jobs.create` to run BigQuery jobs.
        *   `bigquery.dataEditor` on `project.dataset` to read from `dwh_vertrag_cache`, write to `fos_tabelle`, and manage `job_log` and `job_status` tables.
    *   **User/Group Permissions**: Any users or groups responsible for monitoring or manually triggering the job will need appropriate BigQuery roles (e.g., `bigquery.viewer`, `bigquery.jobUser`).
4.  **Orchestrator Setup (e.g., Cloud Composer)**:
    *   If a Cloud Composer environment is not already provisioned, create one.
    *   Configure a BigQuery connection within the Cloud Composer environment, ensuring it uses the service account with the correct IAM permissions.
    *   Deploy the Airflow DAG file (e.g., `ausd_bp_ta_cntrct_dist_dag.py`) to the Cloud Composer environment's DAGs folder. This DAG will contain the logic to invoke the `ausd_bp_ta_cntrct_dist_wrapper` stored procedure.
5.  **Initial Data Load / Verification**:
    *   Ensure that the `project.dataset.dwh_vertrag_cache` table contains relevant and up-to-date data for the job to process.
6.  **Secrets Management**:
    *   If any sensitive parameters (though none are explicitly identified in the provided design) are introduced during the orchestrator setup or BigQuery procedure calls, ensure they are securely managed using Google Secret Manager and accessed appropriately by the orchestrator.

## 5. Known gaps & unresolved references

The following items have been identified as gaps or require further follow-up:

*   **Core Script Logic (`k_ausd_bp_ta_cntrct_dist.ksh`) Detail**: The precise data extraction, transformation, and load (ETL) logic within the original `k_ausd_bp_ta_cntrct_dist.ksh` script was not fully detailed in the design document. The `ausd_bp_ta_cntrct_dist_core` BigQuery Stored Procedure currently contains placeholder logic (e.g., `src.*` in the `INSERT` statement). A separate, in-depth analysis of this original core script is required to accurately translate its full business logic into BigQuery SQL for production readiness. This is a **B4 item** (Blocker for Go-Live).
*   **Helper Script Specifics**: While the general functions of the sourced helper scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) are understood, any highly complex, custom, or unusual logic within them would need careful review and re-implementation in BigQuery SQL or potentially as BigQuery User-Defined Functions (UDFs) if standard SQL functions are insufficient.
*   **Orchestration Context and UC4 Migration**: The interaction with the UC4 scheduler implies that this job is part of a larger job stream. The migration of this single job assumes a broader strategy for migrating the entire UC4 landscape to a modern orchestrator like Cloud Composer. The full impact on upstream/downstream dependencies within the UC4 ecosystem needs to be assessed.
*   **Performance Optimization**: While BigQuery is highly performant, the initial migration might not be fully optimized for specific data volumes or query patterns. Performance monitoring and potential tuning (e.g., partitioning, clustering, query optimization) will be required post-migration.

## 6. Validation

Validation ensures the migrated job functions correctly and produces the expected output.

**How to run the tests:**

1.  **Manual Execution (BigQuery Console/CLI)**:
    *   Execute the `project.dataset.ausd_bp_ta_cntrct_dist_wrapper` stored procedure directly in the BigQuery console or via the `bq query` command-line tool.
    *   Provide test parameters for `p_stichtag` (e.g., `'01012023'`) and `p_wiederanlaufwert` (e.g., `0` or a specific `DWH_VERTRAG_ID`).
    *   Example: `CALL project.dataset.ausd_bp_ta_cntrct_dist_wrapper('01012023', 0);`
2.  **Orchestrator Trigger (Cloud Composer/Workflows)**:
    *   Trigger the corresponding Airflow DAG (e.g., `ausd_bp_ta_cntrct_dist_dag.py`) in the Cloud Composer UI. This will simulate the production scheduling.

**What "passing" means:**

A successful validation run meets the following criteria:

1.  **Job Status**: Query the `project.dataset.job_status` table. There should be a new entry for `job_name = 'ausd_bp_ta_cntrct_dist'` with `status = 'OK'`.
2.  **Detailed Logs**: Query the `project.dataset.job_log` table.
    *   Verify that the log contains informational messages indicating the job started, core logic was called, and the job completed successfully.
    *   Crucially, there should be **no entries with `log_level = 'ERROR'`**.
3.  **Data Verification**:
    *   Query the target table `project.dataset.fos_tabelle`.
    *   Verify that the data inserted into `fos_tabelle` matches the expected output based on the `p_stichtag` and `p_wiederanlaufwert` parameters.
    *   Confirm that the `DWH_VERTRAG_ID` filtering for restart logic (`dwh_vertrag_id > v_restart_value`) and the date filtering (`Gueltig_von <= Stichtag < Gueltig_bis AND LADEDATUM < Stichtag`) are correctly applied.
    *   Compare a sample of processed records with the output of the original KornShell script (if possible) to ensure data fidelity.
4.  **No External Errors**:
    *   Check BigQuery job history for any failed jobs related to the stored procedure execution.
    *   Review Cloud Composer/Workflows logs for any errors or unexpected behavior during DAG execution.

## 7. Rollback procedure

In the event that the migrated job fails validation or encounters critical issues in production, the following rollback procedure should be followed to revert to the original KornShell script:

1.  **Stop New Executions**:
    *   Immediately pause or disable the Airflow DAG (or equivalent orchestrator job) that invokes the `project.dataset.ausd_bp_ta_cntrct_dist_wrapper` BigQuery Stored Procedure. This prevents any further execution of the migrated job.
2.  **Re-enable Original Scheduler**:
    *   Re-enable the original UC4 job (`DW.BERT_AUSD_BP_TA_CNTRCT_DIST.xml`) that invokes `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_cntrct_dist.ksh`.
3.  **Verify Original Job Functionality**:
    *   Monitor the execution of the original KornShell script to ensure it is running successfully and processing data as expected. Check its logs and output.
4.  **Data Consistency (Conditional)**:
    *   **If the BigQuery job made irreversible changes or introduced data discrepancies in `project.dataset.fos_tabelle`**: A data rollback or reconciliation strategy might be necessary. This could involve:
        *   Restoring `project.dataset.fos_tabelle` from a previous backup (if available).
        *   Running the original KornShell script with parameters that force a full reload or correct the affected data.
        *   Manually correcting data in `fos_tabelle` if the impact is limited.
    *   **If the BigQuery job did not make any changes or changes were minor/reversible**: This step might be skipped.
5.  **Decommission BigQuery Assets (Post-Rollback)**:
    *   Once the rollback is confirmed successful and the original job is stable, the BigQuery stored procedures (`ausd_bp_ta_cntrct_dist_wrapper`, `ausd_bp_ta_cntrct_dist_core`) and the logging/status tables (`job_log`, `job_status`) can be dropped or archived, depending on organizational policy. This step should only be performed after a full and stable rollback.