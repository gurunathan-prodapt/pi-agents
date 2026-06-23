# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the legacy job `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_opt_text.ksh`.

The original job comprised:
*   A KornShell script (`k_ausd_bp_ta_bpr_opt_text.ksh`) responsible for orchestration, parameter handling, date validation, and invoking an Oracle SQL script.
*   An Oracle SQL script (`d_ausd_bp_ta_bpr_opt_text.sql`) performing core data transformation, specifically truncating and inserting data into the `sof$ta_bpr_opt_text` table by joining `sof$ta_bpr_optionen` and `sof$ta_bpr_beschr`.

The job has been migrated to Google Cloud BigQuery. The KornShell orchestration logic and the Oracle SQL data transformation logic have been consolidated into a single BigQuery Stored Procedure. The target table `sof$ta_ta_bpr_opt_text` has also been created in BigQuery.

## 2. Generated Artifacts

The migration process generated the following BigQuery-compatible artifacts:

*   **`isbert_dataset/tables/sof_ta_bpr_opt_text.sql`**
    *   **Role**: This file contains the Data Definition Language (DDL) for creating the target table `sof_ta_bpr_opt_text` in the `isbert_dataset` BigQuery dataset. This table will store the processed 'Basisprodukt' option text data.

*   **`isbert_dataset/stored_procedures/k_ausd_bp_ta_bpr_opt_text_sp.sql`**
    *   **Role**: This file defines a BigQuery Stored Procedure named `k_ausd_bp_ta_bpr_opt_text_sp`. This stored procedure encapsulates the entire migrated job logic, including:
        *   Receiving and validating input parameters (Job ID, Entry Number, Stichtag, Restart Value).
        *   Deriving necessary date values (today, yesterday).
        *   Determining the `v_datum` from `dwtk_meldungen`.
        *   Truncating the `sof_ta_bpr_opt_text` table.
        *   Inserting data into `sof_ta_bpr_opt_text` from `sof_ta_bpr_optionen` and `sof_ta_bpr_beschr`.
        *   Capturing and logging the number of processed records to a control table.

## 3. Key Design Decisions

*   **Consolidation into a Single BigQuery Stored Procedure**:
    *   **Why**: This approach simplifies deployment, execution, and maintenance. BigQuery Stored Procedures efficiently handle both procedural logic (parameter validation, variable assignment, control flow) and DML operations. It eliminates the need for external orchestration for this specific job, reducing operational overhead and dependencies on shell environments.
    *   **Trade-offs**: For highly complex jobs with distinct, reusable SQL components, a more modular approach (e.g., separate SQL scripts called by an Airflow DAG) might be considered. However, for this job's scope, the tight coupling of orchestration and SQL logic made consolidation beneficial.

*   **Direct BigQuery SQL for Data Transformation**:
    *   **Why**: Leveraging BigQuery's native SQL capabilities ensures optimal performance and scalability for data processing. It avoids intermediate tools or layers that could introduce overhead or complexity.
    *   **Trade-offs**: Required careful translation of Oracle-specific SQL constructs, data types, and functions to their BigQuery equivalents.

*   **Replacement of KornShell Utilities with BigQuery Scripting**:
    *   **Why**: All shell script dependencies (e.g., `getopts`, `gestern.ksh`, `f_alis_msgerr.ksh`) and environment setup complexities are eliminated. Date calculations, parameter validation, and error handling are now integrated directly within the BigQuery SQL scripting environment.
    *   **Trade-offs**: Required reimplementation of shell logic using BigQuery SQL scripting features, which can sometimes be more verbose for simple tasks.

*   **Elimination of Temporary Files for Record Counts**:
    *   **Why**: BigQuery Stored Procedures can directly capture the `@@row_count` from DML statements, providing a robust and integrated way to obtain record counts without relying on file I/O, which is prone to errors and less efficient in a cloud-native environment.
    *   **Trade-offs**: None; this is a significant improvement in reliability and efficiency.

*   **Placeholder for `job_run_control` Table**:
    *   **Why**: To replace the commented-out "Job-Tabelle" updates in the original script and establish a structured, BigQuery-native logging mechanism for job status and metrics. This provides better auditability and operational insights.
    *   **Trade-offs**: The DDL for this control table is inferred and basic in the generated code. It requires explicit finalization and creation based on actual logging requirements.

## 4. Manual Steps Before Go-Live

Before the migrated job can be run in production, the following manual steps must be completed:

1.  **BigQuery Dataset Creation**:
    *   Ensure the BigQuery dataset `isbert_dataset` exists in your target Google Cloud Project. If not, create it.

2.  **Source Table Migration**:
    *   The following Oracle source tables must be migrated and populated with data into BigQuery:
        *   `isbert_schema.dwtk_meldungen` -> `isbert_dataset.dwtk_meldungen`
        *   `sof$ta_bpr_optionen` -> `isbert_dataset.sof_ta_bpr_optionen`
        *   `sof$ta_bpr_beschr` -> `isbert_dataset.sof_ta_bpr_beschr`
    *   This typically involves setting up a data ingestion pipeline (e.g., using Cloud Data Fusion, Dataflow, or batch loading tools) to ensure these BigQuery tables contain up-to-date and accurate data from their Oracle counterparts.

3.  **IAM Permissions**:
    *   The Google Cloud service account or user identity that will execute the BigQuery Stored Procedure must have the necessary IAM roles and permissions. At a minimum, these include:
        *   `bigquery.datasets.get` on the `isbert_dataset`.
        *   `bigquery.tables.create`, `bigquery.tables.update`, `bigquery.tables.getData`, `bigquery.tables.updateData`, `bigquery.tables.truncate` on `isbert_dataset.sof_ta_bpr_opt_text`.
        *   `bigquery.tables.getData` on `isbert_dataset.dwtk_meldungen`, `isbert_dataset.sof_ta_bpr_optionen`, `isbert_dataset.sof_ta_bpr_beschr`.
        *   `bigquery.routines.create` (for initial deployment of the SP) and `bigquery.routines.update` (for subsequent updates) on the `isbert_dataset`.
        *   `bigquery.tables.create` and `bigquery.tables.insertAll` on `isbert_dataset.job_run_control` (for logging).

4.  **Control/Logging Table Creation**:
    *   Manually create the `isbert_dataset.job_run_control` table using the DDL provided in the generated stored procedure or a more comprehensive DDL based on your organization's logging standards.
    *   Example DDL (from generated code):
        ```sql
        CREATE TABLE IF NOT EXISTS `isbert_dataset.job_run_control`
        (
            job_id            STRING,
            run_date          DATE,
            stichtag          DATE,
            records_processed INT64,
            status            STRING,
            start_timestamp   TIMESTAMP,
            end_timestamp     TIMESTAMP
        );
        ```

5.  **Scheduling**:
    *   Configure a scheduler (e.g., Cloud Composer/Airflow, Cloud Scheduler, or a custom solution) to invoke the BigQuery Stored Procedure `isbert_dataset.k_ausd_bp_ta_bpr_opt_text_sp`.
    *   The scheduler must pass the required parameters: `p_job_kennung`, `p_eintrags_nr`, and `p_stichtag` (in `YYYYMMDD` format). The `p_wiederanlauf_wert` parameter is optional and defaults to `0`.

## 5. Known Gaps & Unresolved References

The following items have been identified as potential gaps, require further clarification, or are flagged for follow-up:

*   **Missing Complexity Information**: The original `file_complexity` data was unavailable. This means the actual complexity of the source scripts might be higher than assumed, potentially leading to unforeseen challenges or underestimation of effort.
*   **`gestern.ksh` Logic**: The exact logic of the `gestern.ksh` script (used for date derivation) was assumed to be straightforward date calculation. If it contains complex calendar logic (e.g., handling holidays, fiscal periods), this needs to be explicitly clarified and implemented correctly in BigQuery.
*   **`job_run_control` Table Schema**: The DDL for `isbert_dataset.job_run_control` in the generated code is a basic placeholder. Its schema needs to be finalized and explicitly created to align with organizational logging and auditing requirements.
*   **`trace.sql.cfg` and Spooling**: The original job included `trace.sql.cfg` and spooled output to a trace file. While BigQuery provides logging mechanisms (e.g., Cloud Logging), specific requirements for detailed trace/debug logging need to be defined and implemented if they are critical for operational support.
*   **Environment Variables (`BERT_DIR_ROOT`, `DW_DIR_UTL`, `HOME`)**: These shell environment variables are no longer directly relevant in the BigQuery context. Any logic that implicitly relied on their *values* (e.g., specific file paths) needs to be re-evaluated. If such values are still required, they should be passed as parameters to the stored procedure or configured as constants within it.
*   **Oracle Database Link (`@pcrs1`)**: The migration design assumed that `v_carmen = "@pcrs1"` referred to the same Oracle instance where the source tables resided, and thus direct access to migrated BigQuery tables would suffice. If `pcrs1` referred to a *different* external Oracle database, then data from that external system must also be explicitly migrated and ingested into BigQuery. This is a critical assumption that needs validation.
*   **`isbert_schema.DWPA_UTIL_SKRIPT` Package**: Only the `runstatement` function for `TRUNCATE` was explicitly handled. If other functions or procedures from this Oracle package were implicitly used by the original job or are part of its broader context, they need to be identified and migrated to BigQuery equivalents (e.g., UDFs, stored procedures).
*   **`p_wiederanlauf_wert` Parameter**: This parameter is defined in the BigQuery Stored Procedure but is not explicitly used in the generated logic. Its original purpose (restart logic) needs to be clarified and implemented if it's a required functionality for the job.

## 6. Validation

To ensure the migrated job functions correctly, follow these validation steps:

1.  **Prepare Test Data**:
    *   Ensure all prerequisite BigQuery source tables (`isbert_dataset.dwtk_meldungen`, `isbert_dataset.sof_ta_bpr_optionen`, `isbert_dataset.sof_ta_bpr_beschr`) are populated with representative test data. Ideally, this data should mirror a known state from the legacy Oracle system.

2.  **Execute the BigQuery Stored Procedure**:
    *   Call the stored procedure with valid test parameters. For example:
        ```sql
        CALL `isbert_dataset.k_ausd_bp_ta_bpr_opt_text_sp`(
            p_job_kennung => 'TEST_MIGRATION_JOB',
            p_eintrags_nr => '001',
            p_stichtag    => '20231026', -- Use a relevant date for your test data
            p_wiederanlauf_wert => 0
        );
        ```

3.  **Verify Output Data**:
    *   Query the target table: `SELECT * FROM `isbert_dataset.sof_ta_bpr_opt_text`;`
    *   Check the number of rows and the content of the `CNTRCT_ID`, `BPR_ID`, and `PDS_DESCRIPTION` columns.

4.  **Check Logging**:
    *   Query the control table: `SELECT * FROM `isbert_dataset.job_run_control` WHERE job_id = 'TEST_MIGRATION_JOB' ORDER BY start_timestamp DESC LIMIT 1;`
    *   Verify the entry for the test run.

**What "Passing" Means**:

*   **Successful Execution**: The BigQuery Stored Procedure completes without raising any errors.
*   **Data Accuracy**:
    *   The `isbert_dataset.sof_ta_bpr_opt_text` table is successfully truncated and repopulated.
    *   The number of rows in `isbert_dataset.sof_ta_bpr_opt_text` matches the expected count based on the source data and the logic of the original Oracle job.
    *   The `CNTRCT_ID`, `BPR_ID`, and `PDS_DESCRIPTION` values in the target table are identical to what the original Oracle job would have produced for the same input data. This requires a direct comparison of output from the legacy system with the BigQuery output.
*   **Logging**: A new entry is created in `isbert_dataset.job_run_control` for the test run, with `status = 'SUCCESS'`, an accurate `records_processed` count, and correct `start_timestamp`/`end_timestamp` values.
*   **Parameter Validation**: Test cases with invalid `p_stichtag` formats (e.g., '2023-10-26', 'ABC') or missing mandatory parameters should correctly trigger the `RAISE` statements within the stored procedure.

## 7. Rollback Procedure

In the event of issues detected after go-live, follow this rollback procedure:

1.  **Immediate Action**:
    *   Immediately stop or disable any scheduled executions of the BigQuery Stored Procedure `isbert_dataset.k_ausd_bp_ta_bpr_opt_text_sp`.

2.  **Data Restoration (if necessary)**:
    *   If the `isbert_dataset.sof_ta_bpr_opt_text` table was corrupted or incorrectly populated by the migrated job, assess the impact.
    *   **Option A (Restore from backup)**: Restore `isbert_dataset.sof_ta_bpr_opt_text` from the last known good backup.
    *   **Option B (Re-run previous job)**: If the data is idempotent and the issue is minor, truncate `isbert_dataset.sof_ta_bpr_opt_text` and wait for the legacy system to re-process the data (if it was still active).
    *   **Option C (Manual Correction)**: For small-scale, easily identifiable errors, manual data correction might be an option, but this should be a last resort.

3.  **Revert to Legacy System**:
    *   Re-enable the original KornShell job (`vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_opt_text.ksh`) in the legacy environment.
    *   Ensure the legacy job can process data correctly from its original Oracle source and populate its target table.

4.  **Code Rollback (if necessary)**:
    *   If the issue is determined to be code-related within the BigQuery Stored Procedure, revert `isbert_dataset.k_ausd_bp_ta_bpr_opt_text_sp` to a previous stable version.
    *   If the DDL for `isbert_dataset.sof_ta_bpr_opt_text` caused schema conflicts or issues, consider reverting or modifying it.

5.  **Investigation and Remediation**:
    *   Thoroughly investigate the root cause of the failure in the BigQuery migration.
    *   Implement necessary fixes in the BigQuery code or configuration.
    *   Conduct comprehensive re-testing in a non-production environment before attempting another go-live.