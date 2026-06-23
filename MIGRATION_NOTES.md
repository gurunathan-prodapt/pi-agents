# MIGRATION_NOTES.md

## 1. Summary

The legacy job `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_msisdn.ksh` has been migrated. This job is responsible for provisioning selected base products for the BERT system by extracting contract cache data from the Data Warehouse (DWH) for the Forderungsscoring application.

The migration target platform is Google Cloud Platform (GCP), leveraging:
*   **BigQuery** for data storage and all transformation logic, encapsulated within BigQuery Stored Procedures.
*   **Cloud Composer (Apache Airflow)** or **Workflows** for orchestration, scheduling, and monitoring of the BigQuery procedures.

The original KornShell (ksh) wrapper script, controller script, and core Oracle SQL*Plus script have been translated into a hierarchical set of BigQuery Stored Procedures, maintaining the original job's logical flow and parameter handling.

## 2. Generated artifacts

The following artifacts have been generated as part of this migration:

*   **`bigquery/ddl/bert_schemas.sql`**
    *   **Role:** Defines the necessary BigQuery schemas (tables) for the migrated job. This includes the target table `dwh_bert_dataset.sof_ta_msisdn`, the source tables `dwh_bert_dataset.dwtk_meldungen` and `dwh_bert_dataset.sof_ta_msisdn_his`, and the audit/logging table `dwh_bert_dataset.job_log`.

*   **`bigquery/procedures/d_ausd_bp_ta_msisdn_transform.sql`**
    *   **Role:** This BigQuery Stored Procedure encapsulates the core data transformation logic previously found in `d_ausd_bp_ta_msisdn.sql`. It derives a `v_datum` from `dwtk_meldungen`, truncates the `sof_ta_msisdn` table, and inserts the latest valid MSISDN records from `sof_ta_msisdn_his` using BigQuery SQL.

*   **`bigquery/procedures/k_ausd_bp_ta_msisdn_controller.sql`**
    *   **Role:** This BigQuery Stored Procedure translates the controller logic from `k_ausd_bp_ta_msisdn.ksh`. It receives parameters from the wrapper, performs date validation, and then calls the `d_ausd_bp_ta_msisdn_transform` procedure to execute the core data logic.

*   **`bigquery/procedures/r_ausd_bp_ta_msisdn_wrapper.sql`**
    *   **Role:** This is the main BigQuery Stored Procedure, acting as the entry point for the migrated job, analogous to `r_ausd_bp_ta_msisdn.ksh`. It handles parameter parsing, defaulting (`Stichtag`, `Wiederanlaufwert`), robust logging to `dwh_bert_dataset.job_log`, and orchestrates the call to `k_ausd_bp_ta_msisdn_controller`. It also includes BigQuery-native error handling.

## 3. Key design decisions

*   **Lift-and-Shift of Logic into BigQuery Stored Procedures:** The primary design decision was to translate the existing KornShell and Oracle SQL logic directly into BigQuery Stored Procedures. This approach preserves the original job's layered structure (wrapper -> controller -> core transformation) and business logic flow.
    *   **Why:** This minimizes re-engineering risk, leverages BigQuery's powerful SQL engine for transformations, and allows for a more direct and verifiable migration path. It also centralizes logging within BigQuery, simplifying monitoring.
*   **Hierarchical Procedure Structure:** The three-tiered structure of the original job (wrapper, controller, core SQL) was maintained by creating three corresponding BigQuery Stored Procedures.
    *   **Why:** This modularity enhances readability, maintainability, and debugging, reflecting the original design intent.
*   **BigQuery for Logging and Audit:** File-based logging and error handling from the original ksh scripts have been replaced with inserts into a dedicated BigQuery `job_log` table.
    *   **Why:** Provides a centralized, queryable, and scalable audit trail for job executions, status, and errors, integrating seamlessly with GCP's monitoring tools.
*   **Native BigQuery Functions:** Oracle-specific functions (e.g., `NVL`, `TO_DATE`, `TO_CHAR`) and shell date utilities have been replaced with their BigQuery equivalents (`COALESCE`, `PARSE_DATE`, `FORMAT_DATE`, `CURRENT_DATE()`).
    *   **Why:** Ensures optimal performance and compatibility within the BigQuery environment.
*   **Orchestration via Cloud Composer/Workflows:** The job's execution will be managed by a modern GCP orchestrator.
    *   **Why:** Provides robust scheduling, dependency management, error handling, and monitoring capabilities far superior to traditional cron-based ksh execution.

**Notable Trade-offs:**

*   **Not a Full Re-architecture:** This migration focuses on translating existing logic rather than a complete re-architecture into a purely declarative data pipeline (e.g., using dbt for transformations). While effective for this job, it might not be suitable for all legacy migrations.
*   **Manual Translation of Shell Logic:** The parameter parsing, defaulting, and date validation logic from the ksh scripts required careful manual translation into BigQuery SQL, which can be verbose for complex shell scripts.
*   **`v_datum` Derivation:** The derivation of `v_datum` from `dwtk_meldungen` remains embedded within the core transformation procedure. A more advanced design might decouple this into a metadata service or a separate pre-processing step if `dwtk_meldungen` were frequently used for control parameters across many jobs.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:**
    *   Ensure the BigQuery dataset `dwh_bert_dataset` exists in your GCP project. If not, create it.
2.  **Schema Deployment:**
    *   Execute the DDL script `bigquery/ddl/bert_schemas.sql` in BigQuery to create the `job_log`, `sof_ta_msisdn`, `dwtk_meldungen`, and `sof_ta_msisdn_his` tables.
3.  **Source Data Ingestion:**
    *   Ingest historical and ongoing data from the Oracle source tables `isbert_schema.dwtk_meldungen` and `sof$ta_msisdn_his` into their respective BigQuery tables (`dwh_bert_dataset.dwtk_meldungen`, `dwh_bert_dataset.sof_ta_msisdn_his`). This is a critical prerequisite for the job to function correctly.
4.  **BigQuery Stored Procedure Deployment:**
    *   Deploy the three BigQuery Stored Procedures by executing their respective `CREATE OR REPLACE PROCEDURE` statements in BigQuery:
        *   `bigquery/procedures/d_ausd_bp_ta_msisdn_transform.sql`
        *   `bigquery/procedures/k_ausd_bp_ta_msisdn_controller.sql`
        *   `bigquery/procedures/r_ausd_bp_ta_msisdn_wrapper.sql`
5.  **IAM Permissions Configuration:**
    *   Grant the necessary Identity and Access Management (IAM) permissions to the GCP service account that will be used by Cloud Composer/Workflows to execute the BigQuery procedures. This typically includes:
        *   `BigQuery Data Editor` on `dwh_bert_dataset` (for `sof_ta_msisdn`, `job_log`).
        *   `BigQuery Data Viewer` on `dwh_bert_dataset` (for `dwtk_meldungen`, `sof_ta_msisdn_his`).
        *   `BigQuery Job User` (to run BigQuery jobs/procedures).
6.  **Orchestration Setup (Cloud Composer/Workflows):**
    *   Develop and deploy the Cloud Composer DAG (Python) or Workflows definition (YAML) that will schedule and execute the `dwh_bert_dataset.r_ausd_bp_ta_msisdn_wrapper` BigQuery Stored Procedure.
    *   Configure the DAG/Workflow to pass the required runtime parameters (`p_stichtag_in`, `p_wiederanlaufWert_in`) to the BigQuery procedure.
    *   Ensure appropriate scheduling, retry mechanisms, and alerting are configured within the orchestrator.

## 5. Known gaps & unresolved references

The following items were identified during the migration design and remain as known gaps or areas for potential follow-up/redesign (B4 items):

*   **Schema Confirmation:** The precise DDL (column names, data types, nullability, constraints) for the original Oracle tables (`isbert_schema.dwtk_meldungen`, `sof$ta_msisdn_his`, `sof$ta_msisdn`) was inferred. A detailed DDL review and confirmation against the source system is critical to ensure data integrity and prevent subtle data type or nullability mismatches in BigQuery.
*   **Full Utility Script Analysis:** The full content and potential side effects of the original ksh utility scripts (`.dw_init`, `f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, `gestern.ksh`, `h_alis_sqlplus.ksh`) were not exhaustively analyzed. While core functionalities were translated, there might be hidden dependencies, complex logic, or external system interactions within them that could require specific BigQuery/Python/GCP service translations or alternative solutions.
*   **`starteSQLSkript` Function Logic:** The `starteSQLSkript` function within `h_alis_sqlplus.ksh` was assumed to primarily execute SQL. Its full implementation needs to be reviewed to ensure no other side effects (e.g., specific connection handling, file operations, additional logging, or environment manipulation) were missed during the translation to direct BigQuery procedure calls.
*   **Commented-Out Code:** The `k_ausd_bp_ta_msisdn.ksh` script contains extensive commented-out `sed`, `sort`, and `join` commands. It is critical to confirm whether this functionality is entirely obsolete or represents dormant logic that might need to be reactivated or handled in the migration. If it's obsolete, it should be explicitly documented as such.
*   **Character Encoding:** Special characters in comments and strings in the source scripts suggest a specific character encoding in the legacy environment. This needs to be considered during data ingestion and script conversion to avoid encoding issues in BigQuery, especially if data contains non-ASCII characters.
*   **Performance of Analytic Functions:** While BigQuery supports analytic functions, the performance characteristics might differ from Oracle, especially concerning `PARALLEL` hints which are not directly transferable. Performance testing will be required post-migration to ensure the BigQuery solution meets performance SLAs.

## 6. Validation

To validate the successful migration and functionality of the `r_ausd_bp_ta_msisdn.ksh` job:

**How to Run Tests:**

1.  **Manual BigQuery Execution:**
    *   Execute the top-level wrapper procedure directly in BigQuery:
        ```sql
        CALL dwh_bert_dataset.r_ausd_bp_ta_msisdn_wrapper(
          p_stichtag_in => '01012023', -- Example date (DDMMYYYY)
          p_wiederanlaufWert_in => '0' -- Example value, or NULL for default
        );
        ```
    *   Test with various `p_stichtag_in` values (valid, invalid format, NULL) and `p_wiederanlaufWert_in` values (e.g., '0', '1', NULL).
2.  **Cloud Composer/Workflows Execution:**
    *   Trigger the deployed Cloud Composer DAG or Workflows definition.
    *   Monitor the execution logs within Cloud Composer/Workflows and BigQuery.

**What "Passing" Means:**

*   **Successful Execution:** The `dwh_bert_dataset.r_ausd_bp_ta_msisdn_wrapper` procedure (or the orchestrator task calling it) completes without errors.
*   **Log Verification:** The `dwh_bert_dataset.job_log` table contains an entry for the execution with `status = 'COMPLETED'` and no `ERROR` level messages.
*   **Target Table Population:** The `dwh_bert_dataset.sof_ta_msisdn` table is populated with data.
*   **Data Integrity and Accuracy:**
    *   **Record Count:** The number of records in `dwh_bert_dataset.sof_ta_msisdn` matches the record count in the original Oracle `sof$ta_msisdn` table for the same `Stichtag` and `Wiederanlaufwert` parameters.
    *   **Data Comparison:** A row-by-row comparison (or checksum validation) confirms that the data in `dwh_bert_dataset.sof_ta_msisdn` is identical to the data in the original Oracle `sof$ta_msisdn` table. This is crucial for verifying the correctness of the transformation logic, especially the `MAX(valid_to)` analytic function.
    *   **Edge Cases:** Test with scenarios that might trigger edge cases, such as `dwtk_meldungen` being empty (to verify `v_datum` defaulting to '19000101') or `valid_to` being NULL in `sof_ta_msisdn_his`.
*   **Error Handling Validation:** Intentionally provide invalid input (e.g., `p_stichtag_in => 'INVALID_DATE'`) and verify that the job fails gracefully, logs an `ERROR` message in `job_log`, and the error message is informative.

## 7. Rollback procedure

In the event of critical issues detected post-go-live, the following rollback procedure can be initiated:

1.  **Immediate Deactivation:**
    *   **Action:** Deactivate or pause the Cloud Composer DAG or Workflows definition responsible for scheduling `dwh_bert_dataset.r_ausd_bp_ta_msisdn_wrapper`. This immediately stops any further executions of the migrated job.
2.  **Data Restoration (if necessary):**
    *   **Action:** If data corruption or incorrect data was written to `dwh_bert_dataset.sof_ta_msisdn`, restore the table from a previous known good state. This could involve:
        *   Using BigQuery's time travel feature to query data as of a specific timestamp before the erroneous run.
        *   Restoring from a BigQuery table snapshot or backup if such a strategy is in place.
    *   **Note:** Since the job truncates the target table, a full restore might be necessary if the previous day's data is required.
3.  **Logic Reversion (if necessary):**
    *   **Action:** If the issue is identified as a bug in the BigQuery Stored Procedures, revert the procedures to a previous, stable version. This can be done by re-executing the `CREATE OR REPLACE PROCEDURE` statements with the code from a prior commit.
4.  **Full Rollback to Legacy System:**
    *   **Action:** If the issues are severe and cannot be quickly resolved within the GCP environment, revert to the original on-premise execution of `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_msisdn.ksh`.
    *   **Prerequisites:** Ensure the legacy job and its dependencies are still operational and can be re-enabled without issues.
    *   **Impact:** This means the data flow will temporarily revert to the Oracle-based processing.
5.  **Post-Rollback Analysis:**
    *   **Action:** Conduct a thorough root cause analysis of the issue that necessitated the rollback. Address the identified problems in the BigQuery procedures, data ingestion, or orchestration before attempting another go-live.