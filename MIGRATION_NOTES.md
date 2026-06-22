# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the KornShell (ksh) wrapper script `r_ausd_bp_ta_msisdn_his.ksh`. The original script's primary function is to parse command-line arguments, initialize a custom logging framework, and orchestrate the execution of a core processing script (`k_ausd_bp_ta_msisdn_his.ksh`).

The migration targets Google Cloud Platform, specifically:
*   **Target Platform:** Google BigQuery for the orchestration logic and logging.
*   **Orchestration:** The ksh wrapper logic is translated into a BigQuery Stored Procedure.
*   **Logging:** File-based logging is replaced by a dedicated BigQuery audit table.
*   **Core Logic:** The invocation of the core script (`k_ausd_bp_ta_msisdn_his.ksh`) is replaced by a `CALL` to a separate, migrated BigQuery Stored Procedure (or equivalent data pipeline).
*   **Scheduling:** The original UC4 scheduler dependency will be replaced by Cloud Composer (Apache Airflow) or Cloud Workflows.

## 2. Generated Artifacts

The migration process generates the following files:

*   **`dwmsg_job_audit_table.sql`**
    *   **Role:** This SQL script defines the Data Definition Language (DDL) for the `dwmsg_job_audit` table in BigQuery. This table serves as the central repository for job logging, status updates, and error messages, replacing the original script's file-based logging mechanism and custom `DWMSG_` framework. It captures job ID, name, script name, virtual log file name, cutoff date, status, error messages, and creation timestamp.
*   **`ausd_bp_ta_msisdn_his_wrapper.sql`**
    *   **Role:** This SQL script contains the BigQuery Stored Procedure `project.dataset.ausd_bp_ta_msisdn_his_wrapper`. It is the direct replacement for the `r_ausd_bp_ta_msisdn_his.ksh` KornShell script. Its responsibilities include:
        *   Receiving input parameters (`p_stichtag_in`, `p_wiederanlaufWert_in`).
        *   Determining the cutoff date, defaulting to the system date if not provided.
        *   Performing basic parameter validation.
        *   Generating a unique job entry number and virtual log file name.
        *   Logging job start, completion, and failure events to the `dwmsg_job_audit` table.
        *   Implementing error handling using BigQuery's `BEGIN...EXCEPTION` blocks.
        *   Invoking the core processing logic via a `CALL` statement to `project.dataset.k_ausd_bp_ta_msisdn_his`.
*   **(Placeholder) `k_ausd_bp_ta_msisdn_his.sql` or equivalent**
    *   **Role:** This file (or set of files) would contain the migrated core processing logic originally found in `k_ausd_bp_ta_msisdn_his.ksh`. Its specific form (e.g., BigQuery Stored Procedure, Dataflow job, Dataproc script) depends on the detailed analysis of the core script, which is outside the immediate scope of this wrapper migration.
*   **(Placeholder) `airflow_dag_ausd_bp_ta_msisdn_his.py` or equivalent**
    *   **Role:** This Python script (if using Cloud Composer) would define the Apache Airflow DAG responsible for scheduling and orchestrating the execution of the `ausd_bp_ta_msisdn_his_wrapper` BigQuery Stored Procedure, replacing the original UC4 scheduler.

## 3. Key Design Decisions

*   **BigQuery Stored Procedure for Orchestration:** The KornShell wrapper's orchestration logic (parameter parsing, environment setup, logging, core script invocation) is directly translated into a BigQuery Stored Procedure. This leverages BigQuery's native capabilities for procedural logic and eliminates the need for external compute for this specific wrapper functionality.
*   **Centralized BigQuery Audit Table for Logging:** The custom `DWMSG_` framework and file-based logging are replaced by a structured BigQuery table (`dwmsg_job_audit`). This provides a centralized, queryable, and scalable logging solution, simplifying auditing and monitoring compared to distributed log files.
*   **BigQuery's `BEGIN...EXCEPTION` for Error Handling:** The `trap` mechanism and custom error handling functions (`DWMSG_Fehlerbehandlung`, `DWMSG_MeldeFehler`) are replaced by BigQuery's native `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks. This standardizes error management within the BigQuery environment.
*   **Parameter-Driven Execution:** All dynamic inputs (cutoff date, restart value) are passed as explicit `IN` parameters to the BigQuery Stored Procedure, eliminating reliance on shell-specific argument parsing (`getopts`) and environment variables.
*   **Assumption of Core Script Migration:** A key decision is to treat the core script (`k_ausd_bp_ta_msisdn_his.ksh`) as a separate migration unit. The wrapper's role is to `CALL` this migrated core logic, abstracting its implementation details. This modular approach allows for independent development and testing of the wrapper and core components.
*   **Trade-off: Custom DWMSG Framework:** While the core logging functionality is replicated in the `dwmsg_job_audit` table, the full breadth of the original `DWMSG_` framework (e.g., complex error code mapping, email notifications, specific message formatting) is not fully replicated within the BigQuery Stored Procedure itself. This is a trade-off for simplicity and leveraging BigQuery's native features. Further integration with Cloud Logging and Cloud Monitoring would handle advanced alerting.

## 4. Manual Steps Before Go-Live

Before the migrated solution can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:**
    *   Ensure the target BigQuery dataset (`project.dataset`) exists. If not, create it:
        ```sql
        CREATE SCHEMA `project.dataset` OPTIONS(location='<your_region>');
        ```
2.  **`dwmsg_job_audit` Table Creation:**
    *   Execute the `dwmsg_job_audit_table.sql` script in BigQuery to create the audit table:
        ```sql
        -- From dwmsg_job_audit_table.sql
        CREATE TABLE IF NOT EXISTS `project.dataset.dwmsg_job_audit` (
          job_id INT64 NOT NULL OPTIONS(description="Unique identifier for the job run, derived from max(job_id) + 1 per job_name."),
          job_name STRING NOT NULL OPTIONS(description="Identifier for the type of job (e.g., 'AUSD_BP_TA_MSISDN_HIS')."),
          script_name STRING OPTIONS(description="Name of the script or stored procedure executing the job."),
          log_file STRING OPTIONS(description="Virtual log file name for historical reference."),
          stichtag STRING OPTIONS(description="Cutoff date for the job in DDMMYYYY format."),
          status STRING NOT NULL OPTIONS(description="Current status of the job (e.g., 'STARTED', 'COMPLETED', 'FAILED')."),
          error_message STRING OPTIONS(description="Detailed error message if the job failed."),
          created_at TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the audit entry was created.")
        )
        PARTITION BY
          DATE(created_at)
        CLUSTER BY
          job_name, job_id;
        ```
3.  **Core Script Migration:**
    *   The core processing script (`k_ausd_bp_ta_msisdn_his.ksh`) *must be migrated and deployed* as `project.dataset.k_ausd_bp_ta_msisdn_his` (or its equivalent BigQuery-callable component) before the wrapper can function correctly. This is a critical prerequisite.
4.  **Source Data Migration:**
    *   Migrate the source tables `DWH$TA_C_VERTRAG` and `FOS-Tabelle` to their respective BigQuery counterparts (e.g., `project.dataset.dwh_ta_c_vertrag`, `project.dataset.fos_tabelle`). Ensure data integrity and schema compatibility.
5.  **IAM Permissions:**
    *   The service account or user identity that will execute the `ausd_bp_ta_msisdn_his_wrapper` stored procedure must have the following BigQuery permissions:
        *   `bigquery.routines.create` (for `CREATE OR REPLACE PROCEDURE`)
        *   `bigquery.routines.update` (for `CREATE OR REPLACE PROCEDURE`)
        *   `bigquery.routines.call` (to execute the wrapper and the core procedure)
        *   `bigquery.tables.getData` (to read from `dwmsg_job_audit` for `MAX(job_id)`)
        *   `bigquery.tables.insertData` (to write log entries to `dwmsg_job_audit`)
        *   `bigquery.tables.updateData` (if status updates are done via `UPDATE` instead of `INSERT` for `COMPLETED` status)
        *   Permissions to read from `project.dataset.dwh_ta_c_vertrag` and write to `project.dataset.fos_tabelle` (these are typically handled by the *core* procedure, but the calling identity might need them if the wrapper directly interacts).
6.  **Scheduling Configuration:**
    *   Configure a new job in Cloud Composer (Airflow DAG) or Cloud Workflows to replace the UC4 scheduler. This job will be responsible for calling the `project.dataset.ausd_bp_ta_msisdn_his_wrapper` stored procedure with the appropriate parameters.
7.  **Downstream System Integration:**
    *   Update any downstream systems (e.g., `FOS-Loader`) that consume data from `FOS-Tabelle` to point to the new BigQuery table (`project.dataset.fos_tabelle`).

## 5. Known Gaps & Unresolved References

*   **Core Script Logic (`k_ausd_bp_ta_msisdn_his.ksh`):** The detailed migration of the core script is a significant unresolved item. This document assumes it will be migrated to a BigQuery Stored Procedure (`project.dataset.k_ausd_bp_ta_msisdn_his`), but its actual implementation might require other GCP services (e.g., Dataflow, Dataproc) if it involves complex file I/O, external system interactions, or non-SQL logic. The success of this wrapper migration is contingent on the successful migration of the core script.
*   **Full `DWMSG_` Framework Replication:** While basic logging is covered, the full functionality of the original `DWMSG_` framework (e.g., specific error codes, email notifications, detailed message formatting beyond `@@error.message`) is not fully replicated. This might require further integration with Cloud Logging, Cloud Monitoring, and Cloud Functions for advanced alerting.
*   **`BERT_DIR_ROOT` and `.dw_init` Environment:** The original script relied on a specific directory structure and environment variables sourced from `.dw_init`. In BigQuery, these are replaced by explicit parameters or hardcoded project/dataset/table names. Any critical configurations previously managed by these environment variables must be explicitly passed or configured within the BigQuery environment.
*   **`DWH_VERTRAG_ID`:** This is a column reference mentioned in the design document. Its exact data type, constraints, and indexing in BigQuery need to be carefully considered during the `DWH$TA_C_VERTRAG` table migration to ensure performance and data integrity.
*   **Missing Complexity/Automation Rates:** The absence of automated complexity and automation rate metrics for the source script means the migration effort was estimated manually. There might be hidden complexities in the original script that could impact the migration of the core logic.

## 6. Validation

To validate the migrated `ausd_bp_ta_msisdn_his_wrapper` stored procedure:

1.  **Prerequisites:**
    *   Ensure the `dwmsg_job_audit` table is created.
    *   Ensure a *mock* or *actual* `project.dataset.k_ausd_bp_ta_msisdn_his` stored procedure exists and can be called. For initial testing of the wrapper, a simple mock procedure that just logs its parameters and returns successfully (or fails on purpose) can be used.
    *   Ensure necessary IAM permissions are in place.

2.  **Test Cases:**

    *   **Successful Execution (Default Stichtag):**
        ```sql
        CALL `project.dataset.ausd_bp_ta_msisdn_his_wrapper`(NULL, NULL);
        ```
        *   **Passing Criteria:**
            *   The call completes without error.
            *   Two entries are found in `project.dataset.dwmsg_job_audit` for `job_name = 'AUSD_BP_TA_MSISDN_HIS'` with the latest `job_id`: one with `status = 'STARTED'` and one with `status = 'COMPLETED'`.
            *   The `stichtag` in the audit entries matches `FORMAT_DATE('%d%m%Y', CURRENT_DATE())`.
            *   The `v_wiederanlaufWert` passed to `k_ausd_bp_ta_msisdn_his` was `0`.
            *   The `k_ausd_bp_ta_msisdn_his` procedure was successfully called.

    *   **Successful Execution (Explicit Stichtag and Wiederanlaufwert):**
        ```sql
        CALL `project.dataset.ausd_bp_ta_msisdn_his_wrapper`('01012023', 123);
        ```
        *   **Passing Criteria:**
            *   The call completes without error.
            *   Two entries are found in `project.dataset.dwmsg_job_audit` for `job_name = 'AUSD_BP_TA_MSISDN_HIS'` with the latest `job_id`: one with `status = 'STARTED'` and one with `status = 'COMPLETED'`.
            *   The `stichtag` in the audit entries is `'01012023'`.
            *   The `v_wiederanlaufWert` passed to `k_ausd_bp_ta_msisdn_his` was `123`.
            *   The `k_ausd_bp_ta_msisdn_his` procedure was successfully called.

    *   **Error Handling (Missing Stichtag - though `NULL` defaults to system date, an empty string would fail):**
        ```sql
        CALL `project.dataset.ausd_bp_ta_msisdn_his_wrapper`('', NULL);
        ```
        *   **Passing Criteria:**
            *   The call fails with an error message containing "Required parameter Stichtag is missing or empty."
            *   One entry is found in `project.dataset.dwmsg_job_audit` for `job_name = 'AUSD_BP_TA_MSISDN_HIS'` with the latest `job_id` and `status = 'FAILED'`, and `error_message` containing the expected message.

    *   **Error Handling (Core Procedure Failure):**
        *   Modify the *mock* `k_ausd_bp_ta_msisdn_his` procedure to intentionally `SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Core procedure failed';`.
        ```sql
        CALL `project.dataset.ausd_bp_ta_msisdn_his_wrapper`('01012023', NULL);
        ```
        *   **Passing Criteria:**
            *   The call fails with an error message containing "Core procedure failed".
            *   Two entries are found in `project.dataset.dwmsg_job_audit` for `job_name = 'AUSD_BP_TA_MSISDN_HIS'` with the latest `job_id`: one with `status = 'STARTED'` and one with `status = 'FAILED'`. The `error_message` in the `FAILED` entry should contain "Core procedure failed".

## 7. Rollback Procedure

In case of critical issues or unexpected behavior after deployment, the following steps outline the rollback procedure to revert to the original KornShell-based system:

1.  **Stop New Invocations:**
    *   Immediately disable or remove the new Cloud Composer DAG or Cloud Workflows job that calls `project.dataset.ausd_bp_ta_msisdn_his_wrapper`.
    *   Re-enable the original UC4 scheduler job for `r_ausd_bp_ta_msisdn_his.ksh`.

2.  **Revert Data Flow (if necessary):**
    *   If the core script migration involved changes to data targets (e.g., `FOS-Tabelle` was written to by the new system), ensure that any downstream consumers (like `FOS-Loader`) are reconfigured to read from the original source tables.
    *   If data was modified or generated incorrectly by the new system, a data rollback or correction might be necessary. This is highly dependent on the nature of the core script's operations and should be planned in detail during its migration. For the wrapper itself, it primarily orchestrates, so direct data impact is minimal.

3.  **Disable/Delete BigQuery Artifacts (Optional but Recommended for Clean-up):**
    *   The `ausd_bp_ta_msisdn_his_wrapper` stored procedure can be dropped:
        ```sql
        DROP PROCEDURE IF EXISTS `project.dataset.ausd_bp_ta_msisdn_his_wrapper`;
        ```
    *   The `dwmsg_job_audit` table can be retained for historical logging or dropped if no longer needed:
        ```sql
        DROP TABLE IF EXISTS `project.dataset.dwmsg_job_audit`;
        ```
    *   Any migrated core procedures (`project.dataset.k_ausd_bp_ta_msisdn_his`) should also be dropped.

4.  **Verify Original System Functionality:**
    *   Confirm that the original `r_ausd_bp_ta_msisdn_his.ksh` script, invoked by UC4, is running as expected and producing correct outputs.
    *   Monitor the original system's logs and outputs to ensure full operational recovery.

This rollback procedure aims to quickly restore the previous operational state, minimizing disruption.