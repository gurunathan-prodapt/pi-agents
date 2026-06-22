# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `r_ausd_v_ta_cntrct_crs.ksh`. This script served as an orchestration and wrapper layer for a contract data reconciliation process, handling parameter parsing, environment setup, logging, error handling, and the invocation of a core data processing script (`k_ausd_v_ta_cntrct_crs.ksh`).

The job has been re-platformed to Google Cloud Platform (GCP), leveraging:
*   **Google BigQuery Stored Procedures:** To encapsulate the wrapper logic (parameter handling, job metadata, error handling) and to host the core reconciliation logic.
*   **Google BigQuery Tables:** For centralized auditing and logging of job executions.
*   **Google Cloud Composer (Apache Airflow):** For scheduling and orchestrating the BigQuery Stored Procedure, replacing the legacy scheduler.

## 2. Generated artifacts

The migration process generated the following artifacts:

*   **`bq_ddl_job_audit_log.sql`**
    *   **Role:** BigQuery Data Definition Language (DDL) script. This creates the `job_audit_log` table in BigQuery, which is used to record the start, end, status, and any errors of job executions. It replaces the file-based logging and `DWMSG_*` framework functions from the legacy system.

*   **`sp_vertragsdatenabgleich.sql`**
    *   **Role:** BigQuery Stored Procedure. This procedure acts as the primary wrapper, mirroring the functionality of the original `r_ausd_v_ta_cntrct_crs.ksh` script. It handles:
        *   Parameter validation (`p_s` for Stichtag, `p_l` for Laufnummer).
        *   Initialization and logging of job execution details to `job_audit_log`.
        *   Error trapping and logging for both parameter issues and failures in the core procedure.
        *   Invocation of the core reconciliation logic via `sp_ausd_v_ta_cntrct_crs`.

*   **`sp_ausd_v_ta_cntrct_crs.sql`**
    *   **Role:** BigQuery Stored Procedure (Placeholder). This procedure is intended to contain the core business logic for contract data reconciliation, which was originally found in `k_ausd_v_ta_cntrct_crs.ksh`. It is currently a placeholder and requires full implementation based on the detailed analysis of the legacy core script.

*   **`dag_vertragsdatenabgleich.py`**
    *   **Role:** Apache Airflow Directed Acyclic Graph (DAG). This Python script defines the workflow for scheduling and executing the `sp_vertragsdatenabgleich` BigQuery Stored Procedure. It replaces the legacy scheduling mechanism and provides robust orchestration capabilities within Cloud Composer. It passes dynamic parameters (`ds` for Stichtag, `ts_nodash` for Laufnummer) to the BigQuery procedure.

## 3. Key design decisions

*   **Re-platforming to BigQuery Stored Procedures:** The decision to migrate the KornShell script's logic into BigQuery Stored Procedures was made to leverage BigQuery's native capabilities for data processing, scalability, and integration within the GCP ecosystem. This aligns with a cloud-native data warehousing strategy.
*   **Separation of Wrapper and Core Logic:** The original script `r_ausd_v_ta_cntrct_crs.ksh` acted as a wrapper for `k_ausd_v_ta_cntrct_crs.ksh`. This pattern was preserved by creating `sp_vertragsdatenabgleich` (wrapper) to call `sp_ausd_v_ta_cntrct_crs` (core). This promotes modularity, easier maintenance, and clearer separation of concerns.
*   **Centralized Audit Logging:** Replacing disparate file-based logs and the `DWMSG_*` framework with a dedicated `job_audit_log` BigQuery table provides a centralized, queryable, and scalable solution for monitoring job execution, status, and errors.
*   **Cloud Composer for Orchestration:** Apache Airflow on Cloud Composer was chosen to replace the legacy scheduler. This provides a modern, robust, and feature-rich platform for workflow management, scheduling, dependency handling, and monitoring, which is well-integrated with other GCP services.
*   **Parameter Handling in BigQuery SP:** The `getopts` logic from the KornShell script was re-engineered into `IF` statements and `RAISE` error handling within the `sp_vertragsdatenabgleich` procedure, ensuring robust parameter validation at the BigQuery level.
*   **Trade-offs:**
    *   **Shell-specific features:** Direct emulation of shell `trap` commands and environment sourcing is not possible in BigQuery. These required re-engineering into BigQuery's `EXCEPTION WHEN ERROR` blocks and explicit variable declarations/configurations.
    *   **Core Logic Placeholder:** The core business logic from `k_ausd_v_ta_cntrct_crs.ksh` is currently a placeholder. This defers the most complex part of the migration and requires further dedicated analysis and development.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:** Ensure the target BigQuery dataset (`project.dataset` as referenced in the code) exists. If not, create it:
    ```bash
    bq mk --dataset project:dataset
    ```
2.  **Deploy `bq_ddl_job_audit_log.sql`:** Execute the DDL script to create the `job_audit_log` table in the target BigQuery dataset.
    ```bash
    bq query --use_legacy_sql=false < bq_ddl_job_audit_log.sql
    ```
3.  **Deploy `sp_vertragsdatenabgleich.sql`:** Deploy the wrapper stored procedure to BigQuery.
    ```bash
    bq query --use_legacy_sql=false < sp_vertragsdatenabgleich.sql
    ```
4.  **Implement and Deploy `sp_ausd_v_ta_cntrct_crs.sql`:**
    *   **Crucial Step:** The placeholder `sp_ausd_v_ta_cntrct_crs.sql` must be fully implemented with the actual business logic from `k_ausd_v_ta_cntrct_crs.ksh`. This will likely involve complex SQL transformations, joins, and DML operations.
    *   Once implemented, deploy this core stored procedure to BigQuery.
    ```bash
    bq query --use_legacy_sql=false < sp_ausd_v_ta_cntrct_crs.sql
    ```
5.  **IAM and Permissions Configuration:**
    *   Ensure the Google Cloud Composer service account has the necessary BigQuery roles (e.g., `BigQuery Data Editor`, `BigQuery Job User`) to create, execute, and manage BigQuery jobs and write to the `job_audit_log` table.
    *   Verify that any service accounts used by the BigQuery procedures (if they interact with other GCP services or external data sources) have appropriate permissions.
6.  **Cloud Composer Environment Setup:**
    *   Upload `dag_vertragsdatenabgleich.py` to the DAGs folder of your Cloud Composer environment.
    *   Update `PROJECT_ID`, `DATASET_ID`, and `BIGQUERY_CONNECTION_ID` variables in `dag_vertragsdatenabgleich.py` to match your GCP environment.
    *   Define the `schedule` for the DAG in `dag_vertragsdatenabgleich.py` (e.g., `@daily`, `0 5 * * *`).
7.  **`ta_cntrct_crs` Table Migration/Accessibility:** Ensure the `ta_cntrct_crs` table (and any other tables referenced by the core logic) is either migrated to BigQuery or accessible by BigQuery as an external table.

## 5. Known gaps & unresolved references

*   **Core Script Content (`k_ausd_v_ta_cntrct_crs.ksh`):** The most significant gap is the full implementation of the core reconciliation logic within `sp_ausd_v_ta_cntrct_crs.sql`. This requires a detailed analysis of the original `k_ausd_v_ta_cntrct_crs.ksh` script to translate its logic accurately into BigQuery SQL. This is a **B4 (Redesign)** item.
*   **Shell-Specific Features:** While the `trap` and environment sourcing mechanisms have been re-engineered, any subtle behaviors or specific environment variables from `$HOME/.dw_init` or utility scripts (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`) that are not explicitly covered by the current BigQuery SP implementation might need further review.
*   **Parameter Usage Details:** The exact meaning and expected format of the `-s` (Stichtag) and `-l` (Laufnummer) parameters, especially how they are used within the core `k_ausd_v_ta_cntrct_crs.ksh` script, need to be fully understood for the `sp_ausd_v_ta_cntrct_crs` implementation. The current Airflow DAG passes `ds` (execution date) for `p_s` and `ts_nodash` (timestamp without dashes) for `p_l`, which might need adjustment based on the core script's expectations.
*   **`ta_cntrct_crs` Table:** The design assumes `ta_cntrct_crs` will be available in BigQuery. Its schema and data migration strategy are not covered here.
*   **Error Codes:** The original script used specific error codes (e.g., 193, 192). While the wrapper SP maps some, a comprehensive mapping of all potential error codes from the core script to BigQuery error handling is needed during the `sp_ausd_v_ta_cntrct_crs` implementation.

## 6. Validation

To validate the migrated job, follow these steps:

1.  **Manual BigQuery Stored Procedure Execution:**
    *   Execute `sp_vertragsdatenabgleich` directly in BigQuery with sample parameters:
        ```sql
        CALL `project.dataset.sp_vertragsdatenabgleich`('2023-10-26', '1234567890');
        ```
    *   **Passing Criteria:**
        *   The procedure completes without throwing an unhandled error.
        *   Query the `job_audit_log` table to confirm an entry with `status = 'SUCCESS'` for the executed `job_id`.
        *   (Once `sp_ausd_v_ta_cntrct_crs` is implemented) Verify that the core logic has performed the expected data transformations or updates in `ta_cntrct_crs` or related tables.
    *   **Error Scenario Validation:** Test with invalid parameters (e.g., `CALL \`project.dataset.sp_vertragsdatenabgleich\`(NULL, '123');` or `CALL \`project.dataset.sp_vertragsdatenabgleich\`('invalid-date', '123');`).
        *   **Passing Criteria:** The procedure should `RAISE` an error, and the `job_audit_log` should record an entry with `status = 'FAILED'` and appropriate `error_code` and `error_detail`.

2.  **Cloud Composer DAG Execution:**
    *   Trigger the `dag_vertragsdatenabgleich` DAG manually from the Airflow UI.
    *   **Passing Criteria:**
        *   The DAG run completes successfully (green status in Airflow UI).
        *   The `call_reconciliation_sp` task completes successfully.
        *   Query the `job_audit_log` table to confirm an entry with `status = 'SUCCESS'` corresponding to the DAG run.
        *   (Once `sp_ausd_v_ta_cntrct_crs` is implemented) Verify the data results as in the manual BigQuery test.
    *   **Error Scenario Validation:** Introduce a controlled error (e.g., temporarily revoke BigQuery permissions from the Composer service account, or modify `sp_ausd_v_ta_cntrct_crs` to intentionally fail).
        *   **Passing Criteria:** The DAG run should fail (red status in Airflow UI), and the `job_audit_log` should record an entry with `status = 'FAILED'` and relevant error details.

## 7. Rollback procedure

In case of critical issues or unexpected behavior after go-live, the following rollback procedure can be initiated:

1.  **Stop New Executions:**
    *   In Cloud Composer, pause the `dag_vertragsdatenabgleich` DAG to prevent any further scheduled runs.
    *   Ensure no manual triggers are initiated.
2.  **Revert BigQuery Stored Procedures (Optional):**
    *   If previous versions of `sp_vertragsdatenabgleich` or `sp_ausd_v_ta_cntrct_crs` exist and are stable, deploy those older versions. If not, the current versions will remain.
3.  **Re-enable Legacy Job:**
    *   Re-enable the original `r_ausd_v_ta_cntrct_crs.ksh` script in its legacy environment.
    *   Restore its original scheduling mechanism.
4.  **Data Rollback (Conditional):**
    *   If the core logic within `sp_ausd_v_ta_cntrct_crs` performed data modifications (INSERT, UPDATE, DELETE, MERGE) that need to be undone, a data rollback strategy must be executed. This could involve:
        *   Restoring `ta_cntrct_crs` (and any other affected tables) from a backup taken before the migration.
        *   Using BigQuery's point-in-time recovery features (time travel) to revert tables to a state before the problematic execution.
        *   Executing specific SQL `DELETE` or `UPDATE` statements to reverse the changes made by the new job.
    *   **Note:** The specific data rollback steps are highly dependent on the implemented logic in `sp_ausd_v_ta_cntrct_crs.sql` and should be defined during its implementation phase.
5.  **Monitor Legacy System:** Verify that the legacy job is running correctly and producing expected results.