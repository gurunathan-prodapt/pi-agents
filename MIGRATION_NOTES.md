# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_period.ksh` and its associated SQL script `d_ausd_v_ta_period.sql` to Google BigQuery.

The original `k_ausd_v_ta_period.ksh` script served as an orchestration and control layer, handling environment setup, parameter validation, and invoking the core data transformation logic contained within `d_ausd_v_ta_period.sql`. The `d_ausd_v_ta_period.sql` script performed data preparation for `ta_period` by reading from `DWTK_MELDUNGEN` and `CDS$TA_PERIOD`, and writing to `SOF$TA_PERIOD` and `VIA`.

The migration re-implements this functionality natively in BigQuery:
*   The orchestration and control logic of `k_ausd_v_ta_period.ksh` has been migrated to a BigQuery stored procedure, `project.dataset.r_ausd_vertrag_control`.
*   The core data transformation logic from `d_ausd_v_ta_period.sql` has been migrated to a separate BigQuery stored procedure, `project.dataset.d_ausd_v_ta_period`.
*   A new BigQuery table, `project.dataset.job_audit`, has been introduced for centralized job logging and status tracking, replacing file-based logging and temporary files.

The target platform for this migration is Google BigQuery, leveraging its native stored procedure capabilities for both orchestration and data transformation.

## 2. Generated Artifacts

The migration process generated the following BigQuery artifacts:

*   **`sql/ddl/job_audit.sql`**
    *   **Role:** This DDL script creates the `job_audit` table in BigQuery. This table serves as a centralized repository for logging the execution status, parameters, processed records, and any error messages for all migrated ETL jobs, including `k_ausd_v_ta_period`. It replaces the disparate file-based logging and temporary file mechanisms of the legacy system.

*   **`sql/procedures/d_ausd_v_ta_period_proc.sql`**
    *   **Role:** This BigQuery stored procedure encapsulates the core data transformation logic originally found in `d_ausd_v_ta_period.sql`. It is responsible for reading data from source tables (`cds$ta_period`, `cds$ta_time_meas_cv`, `cds$ta_description`, `dwtk_meldungen`), applying the necessary transformations, and inserting the processed data into the target table (`sof$ta_period`). It also handles determining the `as_of_date` and includes basic error handling.

*   **`sql/procedures/r_ausd_vertrag_control.sql`**
    *   **Role:** This BigQuery stored procedure acts as the primary orchestration and control script, directly replacing `k_ausd_v_ta_period.ksh`. It handles parameter validation (`p_job_kennung`, `p_eintragsnr`), manages the overall job lifecycle, calls the `d_ausd_v_ta_period` transformation procedure, captures its results (e.g., processed record count), and logs all job activities (start, end, status, errors) to the `job_audit` table.

## 3. Key Design Decisions

The following key design decisions were made during the migration:

*   **Native BigQuery Stored Procedures for Orchestration and Transformation**:
    *   **Decision**: The KornShell orchestration (`k_ausd_v_ta_period.ksh`) and the underlying SQL transformation (`d_ausd_v_ta_period.sql`) were both re-implemented as BigQuery stored procedures (`r_ausd_vertrag_control` and `d_ausd_v_ta_period` respectively).
    *   **Rationale**: This approach leverages BigQuery's native capabilities, eliminating external dependencies on KornShell, `sqlplus`, and legacy shell utilities. It simplifies deployment, execution, and monitoring within the Google Cloud ecosystem.
    *   **Trade-offs**: Requires re-writing shell logic and Oracle SQL into BigQuery SQL, which can be complex for highly procedural or Oracle-specific constructs.

*   **Separation of Orchestration and Transformation Logic**:
    *   **Decision**: The control flow and parameter handling were separated into `r_ausd_vertrag_control`, while the core data manipulation was placed in `d_ausd_v_ta_period`.
    *   **Rationale**: This promotes modularity, reusability, and easier maintenance. The transformation logic can potentially be called independently or by other orchestration processes if needed.
    *   **Trade-offs**: Introduces an additional layer of procedure calls, but the benefits of modularity outweigh this.

*   **Centralized Job Auditing with `job_audit` Table**:
    *   **Decision**: A dedicated BigQuery table (`project.dataset.job_audit`) was created to log all job executions, statuses, parameters, and error details.
    *   **Rationale**: Replaces the scattered file-based logging and temporary files used in the legacy system. Provides a structured, queryable, and centralized source of truth for job monitoring and debugging.
    *   **Trade-offs**: Requires explicit `INSERT` and `UPDATE` statements for logging within the stored procedures, adding a small amount of overhead.

*   **BigQuery Procedure Parameters for Input**:
    *   **Decision**: Command-line parameters (`JobKennung`, `EintragsNr`) from the KornShell script were mapped directly to input parameters of the BigQuery stored procedures.
    *   **Rationale**: Leverages BigQuery's strong typing and parameter handling, making the interface clear and reducing parsing complexity.
    *   **Trade-offs**: Requires external schedulers (e.g., Airflow) to correctly pass these parameters when invoking the BigQuery procedure.

*   **Handling of `starteSQLSkript` and `DWPA_UTIL_SKRIPT` Functionality**:
    *   **Decision**: The implicit logic from `starteSQLSkript` (e.g., job activation/deactivation, `TRUNCATE` before `INSERT`) and `DWPA_UTIL_SKRIPT` was explicitly re-implemented. The `TRUNCATE` operation is now part of `d_ausd_v_ta_period_proc`, and job status tracking is handled via the `job_audit` table.
    *   **Rationale**: Ensures that critical operational aspects of the original job are preserved and made explicit within the BigQuery environment.
    *   **Trade-offs**: Required careful analysis of the implicit behaviors of the legacy wrapper scripts.

*   **Robust Error Handling**:
    *   **Decision**: BigQuery's `EXCEPTION WHEN ERROR THEN` blocks were used to catch and log errors within both stored procedures.
    *   **Rationale**: Provides a structured way to handle runtime errors, log detailed messages to `job_audit`, and ensure the job status is correctly reported as `FAILED`.

*   **Capturing Procedure Results**:
    *   **Decision**: The `r_ausd_vertrag_control` procedure captures the results (e.g., `rows_inserted`) from the `d_ausd_v_ta_period` procedure by iterating over its `SELECT` output using a `FOR record IN (SELECT * FROM TABLE(...))` loop.
    *   **Rationale**: This is the standard BigQuery mechanism for one procedure to consume the `SELECT` output of another, allowing for inter-procedure communication of metrics and status.

## 4. Manual Steps Before Go-Live

Before deploying and running the migrated BigQuery job, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**:
    *   Ensure the target BigQuery dataset `project.dataset` exists.
    *   Ensure the source BigQuery dataset `project.isbert_schema` (for `dwtk_meldungen`) exists.
    *   Ensure the source BigQuery dataset `project.carmen_dataset` (for `cds$ta_period`, `cds$ta_time_meas_cv`, `cds$ta_description`) exists.
    *   *Action*: Create these datasets if they do not already exist.

2.  **Table Migration**:
    *   The following Oracle tables must be migrated to BigQuery:
        *   `DWTK_MELDUNGEN` (to `project.isbert_schema.dwtk_meldungen`)
        *   `CDS$TA_PERIOD` (to `project.carmen_dataset.cds$ta_period`)
        *   `CDS$TA_TIME_MEAS_CV` (to `project.carmen_dataset.cds$ta_time_meas_cv`)
        *   `CDS$TA_DESCRIPTION` (to `project.carmen_dataset.cds$ta_description`)
        *   `SOF$TA_PERIOD` (to `project.dataset.sof$ta_period`)
        *   `VIA` (to `project.dataset.via`) - *Note: The current generated code only writes to `sof$ta_period`. If `VIA` is still a target, it must be created and the `d_ausd_v_ta_period` procedure updated.*
    *   *Action*: Ensure these tables exist in BigQuery with appropriate schemas and data types, and that historical data has been loaded.

3.  **IAM Permissions**:
    *   The service account or user identity that will execute the `r_ausd_vertrag_control` stored procedure must have the following BigQuery IAM roles:
        *   `BigQuery Data Editor` on `project.dataset` (to create/update `job_audit`, `sof$ta_period`, and potentially `via`).
        *   `BigQuery Data Viewer` on `project.isbert_schema` (to read `dwtk_meldungen`).
        *   `BigQuery Data Viewer` on `project.carmen_dataset` (to read `cds$ta_period`, `cds$ta_time_meas_cv`, `cds$ta_description`).
        *   `BigQuery Job User` (to run jobs).
    *   *Action*: Grant these roles to the executing identity.

4.  **Deployment of BigQuery Artifacts**:
    *   Execute `sql/ddl/job_audit.sql` to create the `job_audit` table.
    *   Execute `sql/procedures/d_ausd_v_ta_period_proc.sql` to create the transformation stored procedure.
    *   Execute `sql/procedures/r_ausd_vertrag_control.sql` to create the orchestration stored procedure.
    *   *Action*: Deploy these scripts to your BigQuery environment.

5.  **Configuration of `p_carmen_project` and `p_carmen_dataset`**:
    *   The `r_ausd_vertrag_control` procedure takes `p_carmen_project` and `p_carmen_dataset` as parameters. These are placeholders for the actual BigQuery project and dataset where the `CDS$` source tables reside.
    *   *Action*: Determine the correct project ID and dataset ID for your `CDS$` tables and ensure they are passed correctly when invoking `r_ausd_vertrag_control`.

6.  **Scheduling**:
    *   Integrate the execution of `project.dataset.r_ausd_vertrag_control` into your chosen scheduler (e.g., Cloud Composer/Airflow, Cloud Scheduler, Dataform).
    *   *Action*: Configure the scheduler to invoke the stored procedure with the required parameters (`p_job_kennung`, `p_eintragsnr`, `p_carmen_project`, `p_carmen_dataset`, and optionally `p_as_of_date`).

## 5. Known Gaps & Unresolved References

The following items have been identified as potential gaps or require further investigation/resolution:

*   **`DWPA_UTIL_SKRIPT` Full Functionality**:
    *   The original `d_ausd_v_ta_period.sql` used `PACKAGE:DWPA_UTIL_SKRIPT`. While the `TRUNCATE` before `INSERT` behavior (implied by `starteSQLSkript` and potentially `DWPA_UTIL_SKRIPT`) has been explicitly added to `d_ausd_v_ta_period_proc`, any other specific functions or side effects of `DWPA_UTIL_SKRIPT` are not fully known and may not be replicated.
    *   **Follow-up**: A detailed analysis of `DWPA_UTIL_SKRIPT` is required to ensure all relevant functionality is either re-implemented in BigQuery or confirmed as unnecessary.

*   **Job Activation/Deactivation Logic**:
    *   The original `k_ausd_v_ta_period.ksh` mentioned "aktive Jobs werden ignoriert" and "alte aktive Jobs werden einfach dekativiert". The current BigQuery solution primarily logs job status to `job_audit`. If the "ignore active" or "deactivate old" logic involved more complex operational control (e.g., preventing concurrent runs, marking jobs as inactive in a control table), this is not fully implemented.
    *   **Follow-up**: Clarify the exact requirements for job activation/deactivation. If more than logging is needed, additional logic (e.g., checking `job_audit` for active runs before starting, or updating a separate job control table) must be added to `r_ausd_vertrag_control`.

*   **Parameter Validation (`pruefeParameterGesetzt`)**:
    *   The BigQuery `r_ausd_vertrag_control` procedure includes basic `NULL` or empty string checks for `p_job_kennung` and `p_eintragsnr`. If the original `pruefeParameterGesetzt` helper script contained more sophisticated validation rules (e.g., specific formats, allowed values, range checks), these are not currently replicated.
    *   **Follow-up**: Review the `pruefeParameterGesetzt` script for any complex validation logic and implement it in `r_ausd_vertrag_control` if necessary.

*   **`VIA` Table Target**:
    *   The `MIGRATION DESIGN DOCUMENT` states that `d_ausd_v_ta_period.sql` writes to both `SOF$TA_PERIOD` and `VIA`. However, the generated `d_ausd_v_ta_period_proc.sql` only includes `INSERT` statements for `sof$ta_period`.
    *   **Follow-up**: Confirm if the `VIA` table is still a required target. If so, the `d_ausd_v_ta_period_proc` must be updated to include the necessary DML for `VIA`, and the `VIA` table must be created in BigQuery.

*   **`dwtk_meldungen` Schema**:
    *   The `d_ausd_v_ta_period_proc` assumes the `project.isbert_schema.dwtk_meldungen` table exists with `job_kennung` and `timecreated` columns, which are used to determine `v_as_of_date`.
    *   **Follow-up**: Verify the exact schema of the migrated `dwtk_meldungen` table to ensure these columns exist and are correctly typed.

## 6. Validation

Validation of the migrated job involves several stages to ensure functional equivalence and performance.

### How to Run Tests:

1.  **Unit Test `d_ausd_v_ta_period_proc`**:
    *   Manually invoke `project.dataset.d_ausd_v_ta_period` in BigQuery SQL editor or via a client tool.
    *   Provide various combinations of `p_job_kennung`, `p_target_table_name`, `p_carmen_project`, `p_carmen_dataset`, and `p_as_of_date` (including `NULL` for `p_as_of_date` to test default logic).
    *   Example:
        ```sql
        CALL `project.dataset.d_ausd_v_ta_period`(
            p_job_kennung => 'TEST_JOB',
            p_target_table_name => 'sof$ta_period',
            p_carmen_project => 'your-carmen-project',
            p_carmen_dataset => 'your-carmen-dataset',
            p_as_of_date => '2023-01-01'
        );
        ```
    *   Verify the data inserted into `sof$ta_period` and the returned status/row count.

2.  **Integration Test `r_ausd_vertrag_control`**:
    *   Manually invoke `project.dataset.r_ausd_vertrag_control` with valid and invalid parameters.
    *   Example (valid):
        ```sql
        CALL `project.dataset.r_ausd_vertrag_control`(
            p_job_kennung => 'k_ausd_v_ta_period_test',
            p_eintragsnr => '12345',
            p_carmen_project => 'your-carmen-project',
            p_carmen_dataset => 'your-carmen-dataset',
            p_as_of_date => '2023-01-01'
        );
        ```
    *   Example (invalid - missing parameter):
        ```sql
        CALL `project.dataset.r_ausd_vertrag_control`(
            p_job_kennung => NULL, -- This should trigger an exception
            p_eintragsnr => '12345',
            p_carmen_project => 'your-carmen-project',
            p_carmen_dataset => 'your-carmen-dataset'
        );
        ```
    *   Verify the output of the `CALL` statement and the entries in the `project.dataset.job_audit` table.

3.  **Data Comparison (Functional Equivalence)**:
    *   Run the legacy `k_ausd_v_ta_period.ksh` script for a specific period/dataset.
    *   Run the migrated `r_ausd_vertrag_control` procedure for the *exact same* period/dataset and source data.
    *   Extract the output data from `SOF$TA_PERIOD` (and `VIA` if applicable) from both the legacy Oracle system and BigQuery.
    *   Perform a row-by-row comparison or aggregate comparisons (e.g., `COUNT(*)`, `SUM(column)`, `CHECKSUM`) to ensure data consistency.

### What "Passing" Means:

A successful migration and validation means:

*   **Successful Execution**: The `project.dataset.r_ausd_vertrag_control` procedure completes without unhandled errors.
*   **Correct Logging**: An entry is created in `project.dataset.job_audit` for each execution, with:
    *   `status` = `'SUCCESS'` for successful runs, and `'FAILED'` with a meaningful `error_message` for failed runs.
    *   `job_kennung_param`, `eintragsnr_param` matching the input parameters.
    *   `processed_records` accurately reflecting the number of rows inserted/updated by the transformation.
    *   `start_time` and `end_time` correctly populated.
*   **Data Accuracy**: The data in the target BigQuery tables (`project.dataset.sof$ta_period` and `project.dataset.via` if applicable) is identical to the data produced by the legacy system for the same input, both in terms of content and row count.
*   **Performance**: The BigQuery job completes within acceptable performance thresholds, ideally faster than the legacy system.

## 7. Rollback Procedure

In the event that the migrated BigQuery job encounters critical issues during or after go-live, the following rollback procedure should be followed:

1.  **Stop BigQuery Job Executions**:
    *   Immediately halt any scheduled or manual executions of the `project.dataset.r_ausd_vertrag_control` stored procedure. This can be done by pausing or deleting the scheduler configuration (e.g., Airflow DAG, Cloud Scheduler job).

2.  **Revert to Legacy System**:
    *   Re-enable the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_period.ksh` script in the legacy environment.
    *   Ensure the legacy scheduler is reconfigured to execute the original script.

3.  **Data Cleanup/Restoration (Conditional)**:
    *   **If the BigQuery job performed destructive operations (e.g., `TRUNCATE TABLE`) on target tables (`sof$ta_period`, `via`) and new data has been written**:
        *   Assess the impact. If the data in BigQuery is corrupted or incomplete, restore the affected BigQuery target tables (`project.dataset.sof$ta_period`, `project.dataset.via`) to their state *before* the problematic BigQuery job run. This may involve using BigQuery's time travel feature, restoring from a snapshot, or reloading from a known good backup.
    *   **If the BigQuery job only inserted new data or the target tables were not critical**:
        *   No specific data restoration might be needed, but consider deleting the newly inserted data if it's incorrect.

4.  **Disable/Delete BigQuery Artifacts (Optional)**:
    *   To prevent accidental re-execution or resource consumption, consider disabling or deleting the BigQuery stored procedures (`project.dataset.r_ausd_vertrag_control`, `project.dataset.d_ausd_v_ta_period`) and the `project.dataset.job_audit` table. This step can be deferred if further debugging or analysis of the BigQuery artifacts is planned.

5.  **Post-Rollback Verification**:
    *   Confirm that the legacy `k_ausd_v_ta_period.ksh` script is running successfully and producing correct output in the Oracle environment.
    *   Monitor the legacy system closely for any anomalies.

This rollback procedure ensures a swift return to the stable legacy state while minimizing data loss or corruption.