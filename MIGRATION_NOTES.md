# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of a legacy KornShell script suite, specifically `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_notice.ksh`, to Google BigQuery. The original job was responsible for reconciling contract data related to the `ta_notice` table by extracting records from an Oracle source system, filtering them by a specific cutoff date, and loading them into a target table (`sof$ta_notice`) after truncation.

The migration re-implements this batch process natively within Google Cloud Platform, primarily leveraging BigQuery stored procedures for orchestration and data manipulation. The target platform is Google BigQuery, with orchestration managed by Cloud Composer (Airflow).

## 2. Generated artifacts

The migration process generated the following BigQuery SQL DDL and Stored Procedure definitions:

*   **`project.dataset.sof_ta_notice.sql`**
    *   **Role:** Defines the Data Definition Language (DDL) for the target BigQuery table `sof_ta_notice`. This table will store the reconciled contract data, replacing the legacy `sof$ta_notice` table.
*   **`project.dataset.cds_ta_notice.sql`**
    *   **Role:** Defines a placeholder DDL for the BigQuery table `cds_ta_notice`. This table is intended to be the BigQuery equivalent of the source Oracle `cds$ta_notice` table, from which data is read. Its actual schema should be aligned with the ingested source data.
*   **`project.dataset.dwtk_meldungen.sql`**
    *   **Role:** Defines a placeholder DDL for the BigQuery table `dwtk_meldungen`. This table is the BigQuery equivalent of the Oracle `isbert_schema.dwtk_meldungen` table, used to determine the processing cutoff date. Its actual schema should be aligned with the ingested source data.
*   **`project.dataset.job_log.sql`**
    *   **Role:** Defines the DDL for a new BigQuery audit table `job_log`. This table replaces the legacy file-based logging and will capture detailed execution logs, status updates, and error messages for the migrated job.
*   **`project.dataset.r_ausd_v_ta_notice.sql`**
    *   **Role:** Defines the BigQuery Stored Procedure that replaces the wrapper KornShell script `r_ausd_v_ta_notice.ksh`. This procedure handles job initiation, logging, determines the processing cutoff date, and orchestrates the call to the core logic procedure. It also includes error handling.
*   **`project.dataset.k_ausd_v_ta_notice.sql`**
    *   **Role:** Defines the BigQuery Stored Procedure that replaces the control KornShell script `k_ausd_v_ta_notice.ksh` and the core Oracle SQL script `d_ausd_v_ta_notice.sql`. This procedure performs parameter validation, truncates the target table, executes the `INSERT INTO ... SELECT` logic to populate `sof_ta_notice` from `cds_ta_notice`, and logs record counts.

## 3. Key design decisions

*   **BigQuery Native Re-implementation:** The entire KornShell script suite and Oracle SQL logic have been re-implemented as BigQuery stored procedures. This leverages BigQuery's scalable processing capabilities and eliminates dependencies on legacy shell environments and Oracle SQL*Plus.
*   **Stored Procedure Orchestration:** The wrapper (`r_ausd_v_ta_notice.ksh`) and control (`k_ausd_v_ta_notice.ksh`) scripts are converted into two distinct BigQuery stored procedures (`project.dataset.r_ausd_v_ta_notice` and `project.dataset.k_ausd_v_ta_notice`). The wrapper procedure orchestrates the core logic procedure, mirroring the original script hierarchy.
*   **BigQuery for Logging and Audit:** File-based logging, `spool` commands, and temporary files are replaced by a dedicated BigQuery `job_log` table. This centralizes logging, simplifies auditing, and integrates with Google Cloud's monitoring capabilities.
*   **Cutoff Date Determination:** The logic to determine the processing cutoff date (`v_datum`) from `isbert_schema.dwtk_meldungen` is translated to BigQuery SQL, querying the BigQuery equivalent `project.dataset.dwtk_meldungen` table.
*   **Direct Table Truncation:** The Oracle `isbert_schema.DWPA_UTIL_SKRIPT.runstatement(0, 'TRUNCATE TABLE sof$ta_notice');` call is replaced by a direct `TRUNCATE TABLE` statement within the BigQuery stored procedure, simplifying the operation.
*   **Parameter Handling:** KornShell script parameters and SQL*Plus `DEFINE` variables are replaced by explicit `IN` parameters in the BigQuery stored procedures and `DECLARE` statements for internal variables.
*   **BigQuery SQL Syntax:** Oracle-specific SQL constructs (e.g., `TO_DATE`, `NVL`) are translated to their BigQuery equivalents (e.g., `PARSE_DATE`, `COALESCE`).
*   **BigQuery Error Handling:** Legacy `set -eu`, `trap` commands, and `WHENEVER SQLERROR` are replaced by BigQuery's `BEGIN...EXCEPTION WHEN ERROR THEN...END` blocks, providing structured error management within the stored procedures.
*   **Cloud Composer for Orchestration:** The legacy UC4 scheduler dependency is replaced by Cloud Composer (Airflow), allowing for modern, cloud-native workflow orchestration and dependency management.
*   **Source Data Ingestion Assumption:** The design assumes that the source Oracle `cds$ta_notice` and `isbert_schema.dwtk_meldungen` tables are ingested into BigQuery as `project.dataset.cds_ta_notice` and `project.dataset.dwtk_meldungen` respectively, prior to the execution of this job. The specific ingestion mechanism is considered an external dependency.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps and prerequisites must be completed:

1.  **BigQuery Dataset Creation:**
    *   Ensure the target BigQuery dataset (`project.dataset`) exists. If not, create it.
2.  **BigQuery Table DDL Deployment:**
    *   Execute the DDL scripts for the following tables in the target BigQuery dataset:
        *   `project.dataset.sof_ta_notice.sql`
        *   `project.dataset.cds_ta_notice.sql` (ensure schema matches source Oracle `cds$ta_notice` and ingestion pipeline)
        *   `project.dataset.dwtk_meldungen.sql` (ensure schema matches source Oracle `isbert_schema.dwtk_meldungen` and ingestion pipeline)
        *   `project.dataset.job_log.sql`
3.  **IAM Permissions Configuration:**
    *   Grant necessary BigQuery IAM roles to the service accounts that will execute the stored procedures and the Cloud Composer environment:
        *   `BigQuery Data Editor` on `project.dataset` for `sof_ta_notice` and `job_log` (for `INSERT`, `TRUNCATE`).
        *   `BigQuery Data Viewer` on `project.dataset` for `cds_ta_notice` and `dwtk_meldungen` (for `SELECT`).
        *   `BigQuery Job User` for executing stored procedures.
4.  **Source Data Ingestion Pipeline Setup:**
    *   **Crucial Step:** Implement and configure a robust data ingestion pipeline to continuously or periodically transfer data from the source Oracle `cds$ta_notice` and `isbert_schema.dwtk_meldungen` tables into their respective BigQuery counterparts (`project.dataset.cds_ta_notice` and `project.dataset.dwtk_meldungen`). This could involve:
        *   BigQuery Data Transfer Service (for recurring transfers).
        *   Google Cloud Datastream (for CDC from Oracle).
        *   Custom ETL jobs (e.g., Dataflow, Cloud Functions).
        *   Batch export/load from Oracle to Cloud Storage, then to BigQuery.
    *   Ensure the ingestion frequency and latency meet the business requirements for data freshness.
5.  **BigQuery Stored Procedure Deployment:**
    *   Execute the DDL scripts for the generated BigQuery stored procedures:
        *   `project.dataset.r_ausd_v_ta_notice.sql`
        *   `project.dataset.k_ausd_v_ta_notice.sql`
6.  **Cloud Composer (Airflow) DAG Deployment:**
    *   Develop and deploy an Airflow DAG to your Cloud Composer environment. This DAG should:
        *   Define the schedule for the job.
        *   Include a `BigQueryExecuteStoredProcedureOperator` (or similar) to call `project.dataset.r_ausd_v_ta_notice`.
        *   Pass appropriate values for `p_job_kennung` and `p_eintrags_nr` to the stored procedure.
        *   Replicate any upstream/downstream dependencies that were managed by the legacy UC4 scheduler.
7.  **Secrets Management (if applicable):**
    *   If the data ingestion pipeline or any other component requires database credentials or API keys, ensure these are securely stored in Secret Manager and accessed appropriately by the relevant services.

## 5. Known gaps & unresolved references

The following items have been identified as potential gaps or require further attention:

*   **Oracle DB Link (`@pcrs1`) Replacement:** The design assumes the `cds$ta_notice` data is pre-ingested into BigQuery. The specific implementation and operationalization of this ingestion pipeline (e.g., Data Transfer Service, Datastream, custom ETL) is a critical external dependency that needs to be fully defined, built, and tested. The freshness and latency requirements of the source data must be met by the chosen ingestion method.
*   **Proprietary Shell Utilities:** The original KornShell scripts sourced several custom utility functions (`f_alis_msgerr.ksh`, `h_alis_parameter.ksh`, `h_alis_date.ksh`, `h_alis_sqlplus.ksh`). While their core functionalities (error handling, parameter parsing, date manipulation, SQL execution) have been addressed by BigQuery native features and stored procedure logic, any subtle or complex logic within these utilities might require further analysis to ensure full functional equivalence.
*   **Historical Log Data Migration:** The new `job_log` table replaces file-based logging for future executions. However, the migration of historical log data from the legacy file system into BigQuery for auditing or analysis purposes is not explicitly covered and may be a follow-up item (B4).
*   **Error Handling Behavior:** While BigQuery's `BEGIN...EXCEPTION` blocks provide robust error handling, the exact behavioral nuances (e.g., specific exit codes, retry mechanisms) of the legacy `set -eu`, `trap`, and `WHENEVER SQLERROR` constructs might differ. Thorough testing of error scenarios is required to ensure consistent behavior.
*   **UC4 Orchestration Dependencies:** The migration replaces UC4 scheduling with Cloud Composer. A comprehensive mapping of all UC4 job dependencies, scheduling complexities, and potential inter-job communication is crucial to ensure the new DAG accurately reflects the legacy workflow. Any implicit dependencies or complex scheduling rules need to be explicitly defined in the Airflow DAG.
*   **`dwtk_meldungen` Data Freshness:** The `v_datum` (cutoff date) is derived from `project.dataset.dwtk_meldungen`. It is critical to ensure that the ingestion pipeline for `dwtk_meldungen` is reliable and provides sufficiently fresh data, as the accuracy of this table directly impacts the processing window of the `ta_notice` job.

## 6. Validation

Validation of the migrated job involves several stages to ensure functional equivalence, performance, and reliability.

### How to run the tests:

1.  **Unit Testing BigQuery Stored Procedures:**
    *   Execute `project.dataset.k_ausd_v_ta_notice` directly in BigQuery, providing mock or test `p_job_kennung`, `p_eintrags_nr`, and `v_datum` values.
    *   Execute `project.dataset.r_ausd_v_ta_notice` directly, providing mock parameters.
    *   Verify `job_log` entries for each execution.
2.  **Data Ingestion Verification:**
    *   Confirm that `project.dataset.cds_ta_notice` and `project.dataset.dwtk_meldungen` are populated correctly and are up-to-date according to their ingestion schedules.
    *   Perform row counts and data sampling to ensure data integrity post-ingestion.
3.  **Integration Testing via Cloud Composer:**
    *   Trigger the Cloud Composer DAG that orchestrates the `r_ausd_v_ta_notice` stored procedure.
    *   Monitor the DAG run in the Airflow UI and Cloud Logging for any errors or unexpected behavior.
4.  **Functional Equivalence Testing (Data Comparison):**
    *   Run the legacy `r_ausd_v_ta_notice.ksh` job in the source environment for a specific, controlled cutoff date.
    *   Run the migrated BigQuery job (via Composer or direct SP call) for the *exact same* cutoff date and source data state.
    *   Extract the final data from both the legacy `sof$ta_notice` and the BigQuery `project.dataset.sof_ta_notice` tables.
    *   Perform a row-by-row comparison (e.g., using `EXCEPT` or `MINUS` queries in BigQuery) to identify any discrepancies.
5.  **Performance Testing:**
    *   Measure the execution time of the BigQuery stored procedures and the overall DAG run under typical and peak data volumes.
    *   Compare against the performance of the legacy job.
6.  **Error Handling Testing:**
    *   Simulate various error conditions (e.g., missing source data, invalid parameters, BigQuery service unavailability) to ensure the `BEGIN...EXCEPTION` blocks catch errors, log them correctly in `job_log`, and the DAG handles failures gracefully.

### What "passing" means:

*   **Successful Execution:** The BigQuery stored procedures complete without unhandled errors, and the Cloud Composer DAG runs to completion with a "success" status.
*   **Accurate Logging:** The `project.dataset.job_log` table contains accurate, detailed entries for each job run, including start/end times, parameters, record counts, and any informational or error messages.
*   **Data Integrity and Completeness:** The `project.dataset.sof_ta_notice` table contains all expected records, and no unexpected records, after a full run.
*   **Functional Equivalence:** The data in `project.dataset.sof_ta_notice` is identical to the data produced by the legacy `sof$ta_notice` table for the same input conditions (source data and cutoff date). Any minor, expected differences (e.g., due to data type precision changes) should be documented and accepted.
*   **Performance Acceptance:** The execution time of the migrated job is within acceptable Service Level Agreements (SLAs) and ideally shows improvement over the legacy system.
*   **Robust Error Handling:** Simulated error conditions are caught, logged, and do not lead to data corruption or unrecoverable states. The job fails predictably and provides sufficient information for troubleshooting.

## 7. Rollback procedure

In the event that the migrated job encounters critical issues post-go-live that cannot be immediately resolved, the following rollback procedure should be followed:

1.  **Disable New Orchestration:**
    *   Immediately pause or disable the Cloud Composer DAG responsible for triggering `project.dataset.r_ausd_v_ta_notice`.
2.  **Re-enable Legacy Job:**
    *   Re-enable the original UC4 job for `r_ausd_v_ta_notice.ksh` in the legacy environment.
3.  **Data Rollback (Critical Consideration):**
    *   **Impact:** The migrated job performs a `TRUNCATE TABLE` on `project.dataset.sof_ta_notice` before inserting new data. This means any data loaded by the new job would overwrite previous data.
    *   **Procedure:**
        *   If the `project.dataset.sof_ta_notice` table contains incorrect data due to the new job, it must be restored. This can be done by:
            *   **Option A (Preferred):** Re-running the *legacy* `r_ausd_v_ta_notice.ksh` job to populate the legacy `sof$ta_notice` table, and then re-ingesting this correct data into `project.dataset.sof_ta_notice` via the established ingestion pipeline. This assumes the ingestion pipeline can be triggered on demand or is continuously active.
            *   **Option B (If A is not feasible):** Restoring `project.dataset.sof_ta_notice` from a BigQuery table snapshot or a point-in-time recovery if enabled and configured. This requires prior setup.
            *   **Option C (Manual Intervention):** If neither A nor B is possible, manual data correction or re-loading from a known good source might be necessary, which is highly discouraged due to complexity and risk.
4.  **Revert BigQuery Stored Procedures (if necessary):**
    *   If there were previous versions of the stored procedures, revert `project.dataset.r_ausd_v_ta_notice` and `project.dataset.k_ausd_v_ta_notice` to their last known stable state (e.g., an empty procedure or a previous working version).
5.  **Monitor Legacy System:**
    *   Closely monitor the re-enabled legacy job and its output to ensure it is functioning correctly.

**Note:** Due to the `TRUNCATE TABLE` operation, a data rollback strategy for `project.dataset.sof_ta_notice` must be carefully planned and tested *before* go-live. The most reliable method is to ensure the legacy system can quickly repopulate the data, which can then be re-ingested into BigQuery.