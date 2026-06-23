# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell job `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_c_bfc.ksh`. The original script orchestrated the execution of a SQL script (`d_ausd_v_ta_c_bfc.sql`) for data processing related to the `ta_c_bfc` table, handling parameter validation, job status management, and error reporting.

The job has been migrated to Google Cloud BigQuery. The orchestration logic is re-implemented using BigQuery Stored Procedures, the core data processing logic is translated into BigQuery SQL within a dedicated Stored Procedure, and all data resides in BigQuery tables.

## 2. Generated Artifacts

The migration process generated the following BigQuery artifacts:

*   **`sql/ddl/isbert_dataset_ddl.sql`**
    *   **Role**: This DDL script defines the BigQuery dataset `isbert_dataset` and all necessary tables. It includes the target table `ta_c_bfc`, a staging table `ta_c_bfc_akt`, a `job_status_log` table for tracking job executions, and mock tables for various source dependencies (`dwtk_meldungen`, `ta_cntrct_crs`, `ta_barrier`, `ta_cntrct_valid`, `ta_period`) whose schemas were inferred from their usage in the original SQL.

*   **`sql/procedures/isbert_dataset.bfc_get_bindefrist.sql`**
    *   **Role**: This file defines a BigQuery User Defined Function (UDF) named `bfc_get_bindefrist`. This UDF is a placeholder for the original Oracle function `Cds$vr_Bindefrist.GetBindeFrist` which was called within `d_ausd_v_ta_c_bfc.sql`. It currently returns a dummy date or NULL and requires manual re-implementation of the actual business logic.

*   **`sql/procedures/isbert_dataset.d_ausd_v_ta_c_bfc.sql`**
    *   **Role**: This BigQuery Stored Procedure encapsulates the core data processing logic originally found in `d_ausd_v_ta_c_bfc.sql`. It performs data transformations, populates the staging table `ta_c_bfc_akt`, and then merges data into the final `ta_c_bfc` table, including calls to the `bfc_get_bindefrist` UDF. It also handles the logic for initial population and batched updates.

*   **`sql/procedures/isbert_dataset.r_ausd_ta_c_bfc.sql`**
    *   **Role**: This BigQuery Stored Procedure replaces the original KornShell script `k_ausd_v_ta_c_bfc.ksh`. It serves as the main orchestration entry point, handling parameter validation, job status management (logging start, end, and status in `job_status_log`), and calling the `d_ausd_v_ta_c_bfc` Stored Procedure for data processing. It also includes error handling and updates the job status accordingly.

## 3. Key Design Decisions

*   **BigQuery Stored Procedures for Orchestration**: The KornShell script's orchestration logic (parameter validation, job status management, SQL execution) was migrated to a BigQuery Stored Procedure (`r_ausd_ta_c_bfc`). This leverages BigQuery's native scripting capabilities, eliminating the need for external shell environments and providing a self-contained, auditable execution within BigQuery.
*   **BigQuery Stored Procedures for Core Logic**: The data processing SQL (`d_ausd_v_ta_c_bfc.sql`) was encapsulated in its own BigQuery Stored Procedure (`d_ausd_v_ta_c_bfc`). This promotes modularity, reusability, and clear separation of concerns between orchestration and data transformation.
*   **Dedicated Job Status Logging Table**: The implicit job status management and error reporting of the original shell script (e.g., comments about ignoring active jobs, deactivating old ones) have been formalized into a `job_status_log` BigQuery table. This provides a structured, queryable record of job executions, statuses, and processed record counts.
*   **Direct Parameter Passing**: Command-line parameters (`-j`, `-f`) from the KornShell script are now passed directly as arguments to the BigQuery Stored Procedures, simplifying invocation and validation.
*   **Native BigQuery Error Handling**: Shell-based error handling (`DWMSG_MeldeFehler`) is replaced by BigQuery's `SIGNAL SQLSTATE` and `EXCEPTION WHEN ERROR` blocks, providing robust error propagation and logging.
*   **Elimination of Temporary Files**: The use of temporary files for capturing record counts in the original script is replaced by BigQuery's ability to capture `ROW_COUNT()` from DML statements or direct `SELECT COUNT(*)` queries, storing results in declared variables within the Stored Procedure.
*   **`TRUNCATE` and `MERGE` for Data Manipulation**: The Oracle-specific `TRUNCATE` and `MERGE` statements were directly translated to their BigQuery equivalents for efficient data loading and upsert operations.
*   **`QUALIFY ROW_NUMBER() OVER(...)` for `ROWNUM` Equivalent**: The Oracle `ROWNUM` clause used for limiting updates was translated to `QUALIFY ROW_NUMBER() OVER(ORDER BY ...)` in BigQuery. This provides a similar limiting mechanism, though the specific ordering for the limit might need review if the original `ROWNUM` implied a specific, non-arbitrary order.

## 4. Manual Steps Before Go-Live

The following manual steps are required before the migrated job can be put into production:

1.  **BigQuery Dataset Creation**: Ensure the `isbert_dataset` BigQuery dataset exists in the target Google Cloud Project. The DDL script (`sql/ddl/isbert_dataset_ddl.sql`) includes `CREATE SCHEMA IF NOT EXISTS`, but manual verification is recommended.
2.  **Source Data Ingestion**: All source tables (`ta_cntrct_crs`, `ta_barrier`, `ta_cntrct_valid`, `ta_period`, `dwtk_meldungen`) and the initial `ta_c_bfc` data must be ingested into their respective BigQuery tables within the `isbert_dataset`. The schemas for these tables in `isbert_dataset_ddl.sql` were inferred and **must be validated and adjusted** against the actual Oracle source schemas.
3.  **Re-implement `bfc_get_bindefrist` UDF**: The `isbert_dataset.bfc_get_bindefrist` UDF (`sql/procedures/isbert_dataset.bfc_get_bindefrist.sql`) is a placeholder. Its logic, which was originally derived from an Oracle package function (`Cds$vr_Bindefrist.GetBindeFrist`), **must be fully re-implemented in BigQuery SQL**. This is a critical business logic component.
4.  **IAM Permissions**: Grant appropriate BigQuery IAM roles (e.g., `BigQuery Data Editor` or `BigQuery Job User` with specific table/dataset permissions) to the service account or user that will execute these BigQuery Stored Procedures.
5.  **External Orchestration Setup**: Configure an external orchestrator (e.g., Cloud Composer/Airflow DAG, Cloud Scheduler, or a custom Cloud Function) to trigger the `isbert_dataset.r_ausd_ta_c_bfc` BigQuery Stored Procedure. This orchestrator will replace the upstream `r_ausd_v_ta_c_bfc.ksh` script. Ensure it passes the required `p_jobkennung` and `p_eintragsnr` parameters.
6.  **`v_bfc_procedure_date` Alignment**: In `d_ausd_v_ta_c_bfc.sql`, the `v_bfc_procedure_date` variable is currently set to `CURRENT_DATE()`. This value should be reviewed and potentially aligned with a specific deployment date or versioning strategy for the `bfc_get_bindefrist` logic, if the original Oracle logic used `all_objects.created` for a package.

## 5. Known Gaps & Unresolved References

*   **`bfc_get_bindefrist` UDF Logic (B4 Item)**: As noted in Section 4, the `isbert_dataset.bfc_get_bindefrist` UDF is a placeholder. The actual business logic from the Oracle `Cds$vr_Bindefrist.GetBindeFrist` package function needs to be thoroughly analyzed and re-implemented in BigQuery SQL. This is the most significant redesign/re-implementation item.
*   **Inferred Source Table Schemas**: The schemas for `dwtk_meldungen`, `ta_cntrct_crs`, `ta_barrier`, `ta_cntrct_valid`, and `ta_period` were inferred from their usage in the original SQL. These **must be verified and corrected** against the actual Oracle source schemas to ensure data integrity and correct type mapping.
*   **`v_bfc_procedure_date` Source**: The original Oracle script derived `v_bfc_procedure_date` from `all_objects.created` for a package. The current BigQuery implementation uses `CURRENT_DATE()`. This needs to be refined to accurately reflect the versioning or deployment date of the `bfc_get_bindefrist` logic, potentially by using a metadata table or a fixed deployment date.
*   **Job Management Logic Granularity**: The `r_ausd_ta_c_bfc` procedure deactivates *all* previous 'RUNNING' jobs for a given `p_jobkennung` before starting a new one. While this aligns with the general intent of "ignoring active jobs" and "deactivating old active jobs" mentioned in the original script's comments, the original implementation might have had more granular control (e.g., based on `p_eintragsnr` or specific job instances). This behavior should be validated against the exact legacy system's job management rules.
*   **Upstream/Downstream Dependencies**: The migration of the upstream `r_ausd_v_ta_c_bfc.ksh` job (which triggers the original `k_ausd_v_ta_c_bfc.ksh`) needs to be completed to ensure the new BigQuery job is triggered correctly. Any downstream dependencies that relied on the output or completion of the original job also need to be updated to consume from BigQuery.

## 6. Validation

To validate the migrated job, follow these steps:

1.  **Deploy Artifacts**:
    *   Execute `sql/ddl/isbert_dataset_ddl.sql` to create the dataset and tables.
    *   Execute `sql/procedures/isbert_dataset.bfc_get_bindefrist.sql` to create the UDF.
    *   Execute `sql/procedures/isbert_dataset.d_ausd_v_ta_c_bfc.sql` to create the data processing procedure.
    *   Execute `sql/procedures/isbert_dataset.r_ausd_ta_c_bfc.sql` to create the orchestration procedure.
2.  **Load Test Data**: Populate the source tables (`isbert_dataset.ta_cntrct_crs`, `isbert_dataset.ta_barrier`, `isbert_dataset.ta_cntrct_valid`, `isbert_dataset.ta_period`, `isbert_dataset.dwtk_meldungen`) with representative test data that mirrors the legacy system's input. Ensure `isbert_dataset.ta_c_bfc` is either empty or contains initial data as expected.
3.  **Execute the Orchestration Procedure**:
    ```sql
    CALL `isbert_dataset.r_ausd_ta_c_bfc`('TEST_JOB_KENNUNG', 'TEST_EINTRAGS_NR');
    ```
    Replace `'TEST_JOB_KENNUNG'` and `'TEST_EINTRAGS_NR'` with appropriate test values.
4.  **Monitor Job Status**: Query the `isbert_dataset.job_status_log` table to observe the job's execution status.
    ```sql
    SELECT * FROM `isbert_dataset.job_status_log` WHERE job_kennung = 'TEST_JOB_KENNUNG' ORDER BY start_timestamp DESC;
    ```
5.  **Verify Output Data**: Query the target table `isbert_dataset.ta_c_bfc` and the staging table `isbert_dataset.ta_c_bfc_akt` (if needed for intermediate checks) to verify the processed data.
    ```sql
    SELECT * FROM `isbert_dataset.ta_c_bfc` LIMIT 100;
    ```

**"Passing" Criteria**:

*   The `CALL isbert_dataset.r_ausd_ta_c_bfc` statement completes without raising any unhandled BigQuery errors.
*   The `job_status_log` table shows an entry for the executed job with `status = 'COMPLETED'` and a non-NULL `end_timestamp`.
*   The `record_count` in `job_status_log` accurately reflects the number of records processed or affected in `ta_c_bfc`.
*   The data in `isbert_dataset.ta_c_bfc` (and `isbert_dataset.ta_c_bfc_akt` if applicable) is identical to the expected output from the legacy system when run with the same input data. This includes verifying all columns, especially `bindefrist`, `bfc_age`, `bfc_count`, and `bfc_procedure`.
*   If the `bfc_get_bindefrist` UDF has been re-implemented, its output for various inputs must match the legacy system's `Cds$vr_Bindefrist.GetBindeFrist` function.

## 7. Rollback Procedure

In case of critical issues or failure during go-live, the following steps can be taken to roll back to the legacy system:

1.  **Disable New Job Trigger**: Immediately disable or remove the external orchestrator (e.g., Cloud Composer DAG, Cloud Scheduler job) that triggers `isbert_dataset.r_ausd_ta_c_bfc`.
2.  **Re-enable Legacy Job**: Re-enable the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_c_bfc.ksh` job in its legacy scheduling system.
3.  **Clean Up BigQuery Artifacts (Optional but Recommended)**:
    *   Drop the BigQuery Stored Procedures:
        ```sql
        DROP PROCEDURE IF EXISTS `isbert_dataset.r_ausd_ta_c_bfc`;
        DROP PROCEDURE IF EXISTS `isbert_dataset.d_ausd_v_ta_c_bfc`;
        ```
    *   Drop the BigQuery UDF:
        ```sql
        DROP FUNCTION IF EXISTS `isbert_dataset.bfc_get_bindefrist`;
        ```
    *   Drop the BigQuery tables. **Caution**: This will delete all data. Ensure data is backed up if needed.
        ```sql
        DROP TABLE IF EXISTS `isbert_dataset.job_status_log`;
        DROP TABLE IF EXISTS `isbert_dataset.ta_c_bfc`;
        DROP TABLE IF EXISTS `isbert_dataset.ta_c_bfc_akt`;
        DROP TABLE IF EXISTS `isbert_dataset.dwtk_meldungen`;
        DROP TABLE IF EXISTS `isbert_dataset.ta_cntrct_crs`;
        DROP TABLE IF EXISTS `isbert_dataset.ta_barrier`;
        DROP TABLE IF EXISTS `isbert_dataset.ta_cntrct_valid`;
        DROP TABLE IF EXISTS `isbert_dataset.ta_period`;
        ```
    *   (Optional) Drop the BigQuery dataset if it was created solely for this migration and no other resources depend on it:
        ```sql
        DROP SCHEMA IF EXISTS `isbert_dataset`;
        ```
4.  **Verify Legacy System Functionality**: Confirm that the legacy job is running as expected and producing correct output.