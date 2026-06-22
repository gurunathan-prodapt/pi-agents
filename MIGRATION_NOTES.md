# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the KornShell orchestrator script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_opt_text.ksh`. This script, responsible for the initial provisioning of selected basic products for the BERT system and creating a snapshot extraction of contract cache data for scoring/FOS, has been migrated to Google Cloud Platform.

The migration target platform is **Google BigQuery** for the core logic and orchestration, with **Cloud Composer (Apache Airflow)** handling scheduling and external invocation. The original script's functionality, including parameter parsing, environment setup, date determination, error logging, and invocation of core transformation logic, has been re-platformed into BigQuery Stored Procedures and orchestrated via an Airflow DAG.

## 2. Generated Artifacts

The migration process generated the following files:

*   **`project/dataset/ddl/job_log_audit.sql`**
    *   **Role:** BigQuery Data Definition Language (DDL) script to create the `job_log_audit` table. This table serves as the centralized logging and auditing mechanism for job executions, replacing the file-based logging of the legacy system. It records job status, parameters, timestamps, and error messages.

*   **`project/dataset/sprocs/ausd_bp_ta_bpr_opt_text_wrapper.sql`**
    *   **Role:** BigQuery Stored Procedure that acts as the main entry point and wrapper for the job. It encapsulates the parameter handling, date defaulting, validation, and audit logging logic originally present in `r_ausd_bp_ta_bpr_opt_text.ksh`. This procedure is responsible for calling the core business logic procedure.

*   **`project/dataset/sprocs/k_ausd_bp_ta_bpr_opt_text.sql`**
    *   **Role:** BigQuery Stored Procedure containing the core data extraction and provisioning logic. This procedure translates the business rules from the original `k_ausd_bp_ta_bpr_opt_text.ksh` (which was invoked by the `r_` script) into BigQuery SQL. It handles the deletion of data based on `p_wiederanlaufWert` and the insertion of contract cache data into the `fos_table`.

*   **`project/dags/ausd_bp_ta_bpr_opt_text_dag.py`**
    *   **Role:** An Apache Airflow Directed Acyclic Graph (DAG) written in Python. This DAG is responsible for orchestrating the execution of the BigQuery Stored Procedures. It defines the job's schedule (or allows manual triggering) and invokes the `ausd_bp_ta_bpr_opt_text_wrapper` procedure, passing necessary parameters.

## 3. Key Design Decisions

The following key design decisions were made during the migration:

*   **Re-platforming to BigQuery Stored Procedures:** The orchestrator nature of the original KornShell script, involving parameter handling, conditional logic, and error management, made BigQuery Stored Procedures a natural fit. This approach allows for direct execution within the data warehouse environment, leveraging BigQuery's scalability and SQL capabilities.
*   **Separation of Wrapper and Core Logic:** Mirroring the original `r_` (orchestrator) and `k_` (core logic) script separation, two distinct BigQuery Stored Procedures (`ausd_bp_ta_bpr_opt_text_wrapper` and `k_ausd_bp_ta_bpr_opt_text`) were created. This promotes modularity, reusability, and clearer separation of concerns. The wrapper handles job control, while the core procedure focuses solely on data transformation.
*   **Centralized Audit Logging:** File-based logging was replaced by a dedicated BigQuery table (`job_log_audit`). This provides a centralized, queryable, and persistent record of job executions, parameters, statuses, and error messages, significantly improving observability and troubleshooting capabilities.
*   **Cloud Composer for Orchestration:** Apache Airflow, managed by Cloud Composer, was chosen for job orchestration. This provides robust scheduling, dependency management, monitoring, and parameter passing capabilities, replacing the ad-hoc scheduling and execution environment of the legacy KornShell script.
*   **Native BigQuery Constructs for Shell Logic:** All shell-specific constructs (e.g., `getopts` for parameter parsing, `if` conditions, `trap` for error handling, custom date functions) were translated into their native BigQuery SQL equivalents (e.g., stored procedure parameters, `IFNULL`, `ASSERT`, `BEGIN...EXCEPTION WHEN ERROR`, `CURRENT_DATE()`, `FORMAT_DATE()`). This minimizes external dependencies and maximizes performance within BigQuery.
*   **Restart/Resume Logic:** The `p_wiederanlaufWert` (restart value) logic was directly translated into the `k_ausd_bp_ta_bpr_opt_text` procedure, allowing for conditional deletion and insertion of data based on this value, preserving the original job's restartability.

## 4. Manual Steps Before Go-Live

Before the migrated job can be put into production, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:**
    *   Ensure the target BigQuery dataset `project.dataset` exists. If not, create it:
        ```bash
        bq mk --dataset project:dataset
        ```

2.  **Source and Target Table Creation/Verification:**
    *   Verify that the source table `project.dataset.contract_cache_source` exists and has the correct schema, including `gültig_von`, `gültig_bis`, `ladedatum`, and `DWH_VERTRAG_ID`.
    *   Verify that the target table `project.dataset.fos_table` exists and has the correct schema, including `DWH_VERTRAG_ID` and other relevant columns.
    *   **Crucially, the DDL for `project.dataset.fos_table` and `project.dataset.contract_cache_source` is NOT part of this migration and must be provided separately.**

3.  **Deploy `job_log_audit` Table DDL:**
    *   Execute the `project/dataset/ddl/job_log_audit.sql` script in BigQuery to create the audit log table:
        ```bash
        bq query --use_legacy_sql=false < project/dataset/ddl/job_log_audit.sql
        ```

4.  **Deploy BigQuery Stored Procedures:**
    *   Execute the `project/dataset/sprocs/ausd_bp_ta_bpr_opt_text_wrapper.sql` and `project/dataset/sprocs/k_ausd_bp_ta_bpr_opt_text.sql` scripts in BigQuery to create the stored procedures:
        ```bash
        bq query --use_legacy_sql=false < project/dataset/sprocs/ausd_bp_ta_bpr_opt_text_wrapper.sql
        bq query --use_legacy_sql=false < project/dataset/sprocs/k_ausd_bp_ta_bpr_opt_text.sql
        ```

5.  **IAM Permissions:**
    *   Ensure the Google Cloud service account used by Cloud Composer (or the user executing the DAG) has the necessary BigQuery permissions:
        *   `BigQuery Data Editor` on `project.dataset` (to insert/delete data in `fos_table` and `job_log_audit`).
        *   `BigQuery Data Viewer` on `project.dataset` (to read from `contract_cache_source` and `job_log_audit`).
        *   `BigQuery Job User` (to run BigQuery jobs, including stored procedures).

6.  **Airflow Connection:**
    *   Verify that the `google_cloud_default` connection is configured correctly in your Airflow environment, pointing to the appropriate Google Cloud project.

7.  **Deploy Airflow DAG:**
    *   Upload the `project/dags/ausd_bp_ta_bpr_opt_text_dag.py` file to your Cloud Composer environment's DAGs folder. Airflow will automatically detect and parse it.

8.  **Scheduling Configuration:**
    *   Review and set the desired `schedule` for the `ausd_bp_ta_bpr_opt_text_dag` within the Airflow UI. The generated DAG currently has `schedule=None`, meaning it will only run manually.

## 5. Known Gaps & Unresolved References

The following items are identified as known gaps or require further attention (B4 items):

*   **Full Translation of `k_ausd_bp_ta_bpr_opt_text.ksh` Logic (B4 Item):** The `k_ausd_bp_ta_bpr_opt_text.sql` stored procedure currently contains placeholder logic (`col1, col2, ...`). The complete and accurate translation of the original `k_ausd_bp_ta_bpr_opt_text.ksh` script's business logic (data extraction, filtering, transformations, and insertion into the FOS table) is critical and requires detailed analysis of the original script's content. This is the most significant B4 item.
*   **Schema Definition for Source and Target Tables (B4 Item):** The exact schemas for `project.dataset.contract_cache_source` and `project.dataset.fos_table` (including columns like `DWH_VERTRAG_ID`, `gültig_von`, `gültig_bis`, `ladedatum`) are inferred. These must be explicitly defined and verified to ensure data type compatibility and correct column mapping.
*   **Date Format Mismatch in Airflow DAG:** The Airflow DAG passes `p_stichtag` using `{{ ds_nodash }}`, which resolves to `YYYYMMDD` format. However, the BigQuery Stored Procedure's `PARSE_DATE('%d%m%Y', p_stichtag)` expects `DDMMYYYY`. This is a critical mismatch that will cause runtime errors.
    *   **Resolution:** The Airflow DAG should format the date as `DDMMYYYY` (e.g., `{{ macros.ds_format(ds, "%Y-%m-%d", "%d%m%Y") }}`) or the BigQuery procedure should be updated to `PARSE_DATE('%Y%m%d', p_stichtag)`.
*   **Missing Complexity and Automation Rate:** Due to the absence of data in `file_complexity` and `automation_rate` tables, the initial assessment of the script's complexity and automation potential was based on static code analysis. This might lead to underestimation of effort if hidden complexities exist within the `k_` script.
*   **Configuration Management:** The original script sourced `.dw_init` and other helper scripts for environment configuration. While some parameters are now passed directly, a comprehensive strategy for managing other potential configuration values (e.g., thresholds, static lookup values) should be considered, possibly using a BigQuery configuration table or Airflow Variables.

## 6. Validation

To validate the successful migration and functionality of the job:

1.  **Manual BigQuery Stored Procedure Execution:**
    *   **Test `k_ausd_bp_ta_bpr_opt_text`:** Manually call the core logic procedure with sample `p_stichtag` and `p_wiederanlaufWert` values.
        ```sql
        CALL `project.dataset.k_ausd_bp_ta_bpr_opt_text`('test_job', '01012023', 1, 0);
        ```
        Verify that data is correctly inserted/deleted in `project.dataset.fos_table` and that entries appear in `project.dataset.job_log_audit`.
    *   **Test `ausd_bp_ta_bpr_opt_text_wrapper`:** Manually call the wrapper procedure.
        ```sql
        CALL `project.dataset.ausd_bp_ta_bpr_opt_text_wrapper`('01012023', 0); -- Test with explicit date
        CALL `project.dataset.ausd_bp_ta_bpr_opt_text_wrapper`(NULL, 0);      -- Test with default date
        ```
        Verify that the wrapper correctly handles parameters, calls the core procedure, and logs status in `project.dataset.job_log_audit`.

2.  **Airflow DAG Execution:**
    *   **Trigger DAG:** From the Airflow UI, manually trigger the `ausd_bp_ta_bpr_opt_text_dag`. Provide `p_stichtag` and `p_wiederanlaufWert` as configuration parameters if needed, or allow defaults.
    *   **Monitor DAG Run:** Observe the DAG run in the Airflow UI. Ensure all tasks complete successfully (green status).
    *   **Check BigQuery Logs:** Query `project.dataset.job_log_audit` for the latest entries related to `ausd_bp_ta_bpr_opt_text`.

**"Passing" means:**

*   All BigQuery Stored Procedures execute without throwing unhandled exceptions.
*   The Airflow DAG completes successfully without any task failures.
*   The `project.dataset.job_log_audit` table contains entries for the job run, with the final status recorded as `OK`.
*   The data in `project.dataset.fos_table` is accurately transformed and loaded, matching the expected output from the legacy system for the given `p_stichtag` and `p_wiederanlaufWert`. This requires a comparison with a known good output from the original script.
*   The restart logic (`p_wiederanlaufWert`) correctly deletes and inserts data as per the original script's behavior.

## 7. Rollback Procedure

In case of issues or critical failures after go-live, the following rollback procedure can be initiated:

1.  **Disable Airflow DAG:**
    *   In the Airflow UI, toggle off the `ausd_bp_ta_bpr_opt_text_dag` to prevent further automated executions.

2.  **Revert to Legacy System:**
    *   Resume execution of the original KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_opt_text.ksh` using its previous scheduling mechanism (e.g., cron).

3.  **Data Rollback (if necessary):**
    *   If the migrated job introduced incorrect data into `project.dataset.fos_table`, restore the table to a previous known good state using BigQuery's time travel feature or a previously taken snapshot/backup.
        *   Example (time travel to 1 hour ago):
            ```sql
            CREATE OR REPLACE TABLE `project.dataset.fos_table` AS
            SELECT * FROM `project.dataset.fos_table` FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR);
            ```
        *   **Note:** This step is highly dependent on the impact of the failure and the data retention policies.

4.  **Remove Migrated BigQuery Objects (optional, for clean-up):**
    *   Drop the BigQuery Stored Procedures:
        ```sql
        DROP PROCEDURE IF EXISTS `project.dataset.ausd_bp_ta_bpr_opt_text_wrapper`;
        DROP PROCEDURE IF EXISTS `project.dataset.k_ausd_bp_ta_bpr_opt_text`;
        ```
    *   Drop the audit log table (if no longer needed for historical reference):
        ```sql
        DROP TABLE IF EXISTS `project.dataset.job_log_audit`;
        ```

5.  **Remove Airflow DAG (optional, for clean-up):**
    *   Delete the `project/dags/ausd_bp_ta_bpr_opt_text_dag.py` file from the Cloud Composer DAGs folder.