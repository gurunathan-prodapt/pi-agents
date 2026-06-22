# MIGRATION_NOTES.md

## 1. Summary

This document outlines the migration of the `k_ausd_v_ta_vertrag_tmp.ksh` job, which orchestrates the population of a temporary contract table (`sof$ta_vertrag_tmp`), from its legacy Oracle/KornShell environment to Google BigQuery.

The original job consisted of:
*   A KornShell script (`k_ausd_v_ta_vertrag_tmp.ksh`) responsible for parameter handling, job control (ignoring active jobs, deactivating older ones), and invoking an Oracle SQL script.
*   An Oracle SQL script (`d_ausd_v_ta_vertrag_tmp.sql`) performing data selection, transformation, and insertion into the `sof$ta_vertrag_tmp` table from various Oracle source tables.

The entire workflow has been re-platformed to **Google BigQuery**. The KornShell orchestration logic has been translated into a BigQuery Stored Procedure, and the Oracle SQL transformation logic has been converted to BigQuery Standard SQL, populating a BigQuery table that serves the same purpose as the original temporary Oracle table.

## 2. Generated artifacts

The migration process generated the following BigQuery artifacts:

*   **DDL for Source Tables (BigQuery Schemas & Tables):**
    *   `isbert_dataset/ddl/dwtk_meldungen.sql`: Defines the `dwtk_meldungen` table in `isbert_dataset`.
    *   `sof_dataset/ddl/ta_cntrct_crs3.sql`: Defines the `ta_cntrct_crs3` table in `sof_dataset`.
    *   `sof_dataset/ddl/ta_bp_ref.sql`: Defines the `ta_bp_ref` table in `sof_dataset`.
    *   `sof_dataset/ddl/ta_inv_acc.sql`: Defines the `ta_inv_acc` table in `sof_dataset`.
    *   `dwh_dataset/ddl/vi_s_rd_segment.sql`: Defines the `vi_s_rd_segment` table in `dwh_dataset`.
    *   `sof_dataset/ddl/ta_notice.sql`: Defines the `ta_notice` table in `sof_dataset`.
    *   `sof_dataset/ddl/ta_barrier_zusgf.sql`: Defines the `ta_barrier_zusgf` table in `sof_dataset`.
    *   `sof_dataset/ddl/ta_cntrct_templ.sql`: Defines the `ta_cntrct_templ` table in `sof_dataset`.
    *   `sof_dataset/ddl/ta_cntrct_valid.sql`: Defines the `ta_cntrct_valid` table in `sof_dataset`.
    *   `sof_dataset/ddl/ta_period.sql`: Defines the `ta_period` table in `sof_dataset`.
    *   `sof_dataset/ddl/ta_vvl_upgrade.sql`: Defines the `ta_vvl_upgrade` table in `sof_dataset`.
    *   `sof_dataset/ddl/ta_apn_ve.sql`: Defines the `ta_apn_ve` table in `sof_dataset`.
    *   `sof_dataset/ddl/ta_action_assoc.sql`: Defines the `ta_action_assoc` table in `sof_dataset`.
    *   `sof_dataset/ddl/vi_c_bfc.sql`: Defines the `vi_c_bfc` table in `sof_dataset`.
    *   *Role:* These DDLs establish the schema for the migrated source data within BigQuery, mirroring the original Oracle table structures.

*   **DDL for Target & Logging Tables (BigQuery Schemas & Tables):**
    *   `target_dataset/ddl/ta_vertrag_tmp.sql`: Defines the target `ta_vertrag_tmp` table in `target_dataset`.
    *   `target_dataset/ddl/error_log.sql`: Defines the `error_log` table for capturing job errors.
    *   `target_dataset/ddl/job_table.sql`: Defines the `job_table` for managing job active status and metadata.
    *   `target_dataset/ddl/job_run_log.sql`: Defines the `job_run_log` table for detailed job execution metrics.
    *   *Role:* These DDLs create the necessary tables for the job's output and internal operational logging within BigQuery.

*   **BigQuery SQL Transformation Logic:**
    *   `target_dataset/sql/d_ausd_v_ta_vertrag_tmp_transformation.sql`: Contains the core `INSERT INTO ... SELECT ... UNION ALL` statement, translated from Oracle SQL to BigQuery Standard SQL.
    *   *Role:* This script performs the data selection, transformation, and insertion into the `target_dataset.ta_vertrag_tmp` table. It is designed to be executed within the orchestration stored procedure.

*   **BigQuery Stored Procedure for Orchestration:**
    *   `target_dataset/sp/sp_k_ausd_v_ta_vertrag_tmp.sql`: Encapsulates the entire job logic, including parameter validation, job control, `v_datum` derivation, truncation of the target table, execution of the transformation logic, and comprehensive logging.
    *   *Role:* This is the main entry point for the migrated job, replacing the original KornShell script.

## 3. Key design decisions

*   **Orchestration Re-platforming (KornShell to BigQuery Stored Procedure):**
    *   **Why:** The original KornShell script was primarily an orchestrator for an Oracle SQL script. Migrating this to a BigQuery Stored Procedure centralizes the entire workflow within BigQuery, eliminating external shell dependencies and leveraging BigQuery's native scripting capabilities for control flow, error handling, and logging. This aligns with the `B0 (retire)` automation bucket, indicating a move away from shell-based orchestration.
    *   **Trade-offs:** While centralizing, it introduces more complex SQL scripting within BigQuery for logic that was previously handled by shell utilities (e.g., parameter parsing, file I/O, environment variable management).

*   **Direct SQL Translation (Oracle SQL to BigQuery Standard SQL):**
    *   **Why:** The core data transformation logic was directly translated from Oracle SQL to BigQuery Standard SQL. This approach minimizes functional changes and leverages BigQuery's highly optimized query engine for data processing. Oracle-specific functions (`NVL`, `DECODE`, `MONTHS_BETWEEN`) were converted to their BigQuery equivalents (`COALESCE`, `CASE`, `DATE_DIFF`).
    *   **Trade-offs:** Requires careful manual review and testing to ensure exact functional equivalence, especially for complex date arithmetic and conditional logic.

*   **Custom BigQuery Tables for Job Control and Logging:**
    *   **Why:** The original job used inferred job tables and temporary files for status, error reporting, and record counts. The migrated solution uses dedicated BigQuery tables (`job_table`, `job_run_log`, `error_log`) for a structured, auditable, and easily queryable record of job executions and errors. This provides better visibility and manageability.
    *   **Trade-offs:** Requires initial setup and maintenance of these logging tables.

*   **`v_datum` Calculation:**
    *   **Why:** The `v_datum` (key date) was derived from `isbert_schema.dwtk_meldungen`. This logic is preserved within the BigQuery Stored Procedure, ensuring the same business logic for determining the processing date.
    *   **Trade-offs:** Relies on the accurate and timely migration of the `dwtk_meldungen` table to BigQuery.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:**
    *   Ensure the following BigQuery datasets exist in your GCP project:
        *   `isbert_dataset`
        *   `sof_dataset`
        *   `dwh_dataset`
        *   `target_dataset`
    *   These can be created via the GCP Console, `bq` CLI, or Terraform.

2.  **BigQuery DDL Deployment:**
    *   Execute all DDL scripts (`*.sql` files under `isbert_dataset/ddl`, `sof_dataset/ddl`, `dwh_dataset/ddl`, and `target_dataset/ddl`) to create the necessary tables and schemas in BigQuery.
    *   Example command for each DDL file:
        ```bash
        bq query --use_legacy_sql=false <path_to_ddl_file>.sql
        ```

3.  **Initial Data Migration:**
    *   Perform a full historical data load from the source Oracle tables into their corresponding BigQuery tables (e.g., `isbert_dataset.dwtk_meldungen`, `sof_dataset.ta_cntrct_crs3`, etc.).
    *   Establish an ongoing data synchronization mechanism (e.g., using Datastream, Dataflow, or custom ETL) to keep the BigQuery source tables up-to-date with the Oracle source system until the Oracle system is fully decommissioned.

4.  **BigQuery Stored Procedure Deployment:**
    *   Execute the `target_dataset/sp/sp_k_ausd_v_ta_vertrag_tmp.sql` script to create the stored procedure in BigQuery.
    *   ```bash
        bq query --use_legacy_sql=false target_dataset/sp/sp_k_ausd_v_ta_vertrag_tmp.sql
        ```

5.  **IAM Permissions Configuration:**
    *   Grant the service account or user that will execute the BigQuery Stored Procedure the necessary IAM roles:
        *   `BigQuery Data Editor` on `isbert_dataset`, `sof_dataset`, `dwh_dataset`, and `target_dataset` (to read source data and write to target/logging tables).
        *   `BigQuery Job User` (to run BigQuery jobs, including stored procedures).

6.  **Initial Job Control Table Population (Optional but Recommended):**
    *   If specific job control entries are expected in `target_dataset.job_table` before the first run, manually insert them. For example, to ensure the job can be activated:
        ```sql
        INSERT INTO `target_dataset.job_table` (job_kennung, eintrags_nr, active_flag, status)
        VALUES ('k_ausd_v_ta_vertrag_tmp', 'DEFAULT', FALSE, 'READY');
        ```

7.  **Scheduling Configuration:**
    *   Configure a scheduling mechanism (e.g., Cloud Scheduler, Cloud Composer DAG) to invoke the `target_dataset.sp_k_ausd_v_ta_vertrag_tmp` stored procedure with the required `p_job_kennung` and `p_eintrags_nr` parameters.

## 5. Known gaps & unresolved references

*   **Lineage Discrepancy:** The automated lineage analysis did not correctly identify the `READS`/`WRITES` for the `d_ausd_v_ta_vertrag_tmp.sql` file due to its dynamic invocation within the KornShell script. This was manually corrected during migration, but highlights a potential blind spot for similar patterns.
*   **Helper Script Logic Detail:** The full, detailed logic of the original KornShell helper scripts (`f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`) was not exhaustively analyzed. The migration assumes standard functionalities for error handling, date utilities, and parameter parsing. Any highly specific or custom logic within these helpers might require further review and translation if not already covered.
*   **`migration_bucket`: B0 (retire):** The original job's classification as `retire` suggests that a direct lift-and-shift might not be the optimal long-term solution. Further redesign or simplification of the business logic might be possible and should be discussed with business stakeholders.
*   **Oracle-Specific SQL Review:** While common Oracle functions were translated, complex or obscure Oracle-specific SQL constructs (e.g., specific package calls like `CV.cntrct_validity_id()`, `VVL`, `AP`, `RD`, `AC`, `BF`) require thorough validation to ensure their BigQuery equivalents (or direct translation of underlying logic) are functionally identical.
*   **`v_datum` Dependency:** The `v_datum` calculation relies on the `isbert_dataset.dwtk_meldungen` table. The accuracy of this calculation depends on the correct and timely population of this BigQuery table.
*   **Data Volume and Performance Optimization:** The original Oracle query used parallel hints. While BigQuery handles parallelism automatically, for very large datasets, further performance tuning (e.g., BigQuery partitioning, clustering, or materialized views) might be necessary to match or exceed Oracle's performance.
*   **`EXECUTE IMMEDIATE` Parameterization:** The `EXECUTE IMMEDIATE` statement within the stored procedure uses string concatenation for the `v_v_datum` parameter. While functional, using the `USING` clause for parameter binding is generally a more secure and readable practice, and could be considered a B4 item for refactoring.
*   **`WHERE` clause filtering with `LEFT JOIN`:** The `WHERE` clause conditions `bp.cntrct_cp2_id = c.cntrct_id` (first `UNION ALL` branch) and `bp.cntrct_cp2_id = c.cntrct_parent` (second `UNION ALL` branch) are applied *after* a `LEFT JOIN` to `sof_dataset.ta_bp_ref`. If `bp.cntrct_cp2_id` is `NULL` for a non-matching `LEFT JOIN` record, these `WHERE` conditions will filter out those records. This behavior needs to be explicitly validated against the original Oracle logic to ensure it matches the intended filtering.

## 6. Validation

Validation of the migrated job involves several stages to ensure functional equivalence and performance.

### 6.1. Unit Tests (SQL Transformation Logic)

*   **Objective:** Verify that the core BigQuery SQL transformation logic (`target_dataset/sql/d_ausd_v_ta_vertrag_tmp_transformation.sql`) produces identical results to the original Oracle SQL script for a given set of input data.
*   **Procedure:**
    1.  Load a representative subset of source data from Oracle into the corresponding BigQuery source tables (`isbert_dataset.*`, `sof_dataset.*`, `dwh_dataset.*`).
    2.  Execute the original Oracle SQL script (`d_ausd_v_ta_vertrag_tmp.sql`) against the Oracle source data and capture its output in `sof$ta_vertrag_tmp`.
    3.  Execute the BigQuery transformation logic (e.g., by running the `INSERT ... SELECT` statement directly) against the BigQuery source tables, populating `target_dataset.ta_vertrag_tmp`.
    4.  Compare the contents of the Oracle `sof$ta_vertrag_tmp` table with the BigQuery `target_dataset.ta_vertrag_tmp` table.
*   **"Passing" Criteria:**
    *   **Row Count:** The number of records in both target tables must be identical.
    *   **Data Content:** All columns for all records must match exactly. This includes data types, values, and NULL handling. A row-by-row comparison query should yield no differences.
    *   **Performance:** The BigQuery transformation should complete within acceptable performance thresholds, ideally matching or improving upon the Oracle execution time.

### 6.2. Integration Tests (BigQuery Stored Procedure)

*   **Objective:** Verify that the BigQuery Stored Procedure (`target_dataset/sp/sp_k_ausd_v_ta_vertrag_tmp.sql`) correctly orchestrates the job, including parameter handling, job control, and logging.
*   **Procedure:**
    1.  Ensure BigQuery source tables are populated with test data.
    2.  Execute the BigQuery Stored Procedure with various valid and invalid parameter combinations (e.g., `p_job_kennung`, `p_eintrags_nr`).
    3.  Simulate concurrent runs or runs with existing active jobs to test the job control logic.
    4.  Introduce scenarios that would cause errors in the transformation logic (e.g., missing data, invalid joins) to test error handling.
*   **"Passing" Criteria:**
    *   **Successful Runs:** For valid inputs, the procedure should complete successfully, populate `target_dataset.ta_vertrag_tmp` correctly (as per unit tests), and update `target_dataset.job_run_log` and `target_dataset.job_table` with `SUCCESS` status and correct metrics.
    *   **Error Handling:** For invalid inputs or internal errors, the procedure should log errors to `target_dataset.error_log`, update `target_dataset.job_table` with `FAILED` status, and raise an appropriate error message.
    *   **Job Control:** The `active_flag` and `status` in `target_dataset.job_table` should accurately reflect the job's state (e.g., `active_flag=TRUE` during execution, `active_flag=FALSE` and `status=COMPLETED` or `FAILED` afterwards; older jobs correctly deactivated).
    *   **Parameter Validation:** The procedure should correctly identify and handle missing or invalid input parameters.

### 6.3. End-to-End Validation

*   **Objective:** Verify the entire migrated workflow, including scheduling, in a production-like environment.
*   **Procedure:**
    1.  Deploy the validated BigQuery artifacts to a staging or pre-production environment.
    2.  Configure the scheduling mechanism (e.g., Cloud Scheduler) to invoke the stored procedure.
    3.  Monitor job execution, logs, and output data over several scheduled runs.
*   **"Passing" Criteria:**
    *   All scheduled runs complete successfully without manual intervention.
    *   Output data in `target_dataset.ta_vertrag_tmp` is consistent and accurate over time.
    *   Logging tables (`error_log`, `job_run_log`, `job_table`) are populated correctly and provide clear operational insights.

## 7. Rollback procedure

In case of critical issues or unexpected behavior after go-live, the following rollback procedure can be initiated:

1.  **Immediate Action: Stop BigQuery Job Execution:**
    *   Disable or pause the BigQuery job scheduler (e.g., Cloud Scheduler job, Cloud Composer DAG) that invokes `target_dataset.sp_k_ausd_v_ta_vertrag_tmp`. This prevents further execution of the migrated job.

2.  **Revert to Original Oracle Job:**
    *   Re-enable the original KornShell script (`k_ausd_v_ta_vertrag_tmp.ksh`) and its associated Oracle SQL script (`d_ausd_v_ta_vertrag_tmp.sql`) in the legacy environment.
    *   Ensure the legacy Oracle database and its components are fully operational and processing data as expected.

3.  **Data Reversion (if necessary):**
    *   If any downstream systems have started consuming data from `target_dataset.ta_vertrag_tmp` and this data is found to be incorrect, coordinate with those systems to revert to consuming data from the original Oracle `sof$ta_vertrag_tmp` table.
    *   If `target_dataset.ta_vertrag_tmp` is purely an internal temporary table for this job and its data is not consumed by other systems, no specific data rollback is required for this table itself.

4.  **BigQuery Artifact Rollback (Optional, for clean-up):**
    *   If the issues are severe and require a complete re-evaluation, the BigQuery Stored Procedure and DDLs can be reverted or dropped.
        *   Drop the stored procedure: `DROP PROCEDURE IF EXISTS `target_dataset.sp_k_ausd_v_ta_vertrag_tmp`;
        *   Drop the target table: `DROP TABLE IF EXISTS `target_dataset.ta_vertrag_tmp`;
        *   Consider dropping or archiving the logging tables (`error_log`, `job_table`, `job_run_log`) if they are no longer needed or contain erroneous data.
    *   Note: Dropping source tables (`isbert_dataset.*`, `sof_dataset.*`, `dwh_dataset.*`) is generally not recommended unless the entire data migration is being rolled back, as these may be used by other migrated jobs.

5.  **Post-Rollback Verification:**
    *   Confirm that the original Oracle job is running successfully and producing correct output.
    *   Verify that any downstream systems are correctly consuming data from the Oracle source.

This procedure ensures a quick return to a stable state using the proven legacy system while the issues with the BigQuery migration are investigated and resolved.