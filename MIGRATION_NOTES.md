# MIGRATION_NOTES.md

## 1. Summary

This migration involved the KornShell script `k_ausd_bp_ta_cntrct_dist.ksh`, which served as an orchestration layer for a data processing workflow. The script was responsible for parameter parsing, validation, and executing a core SQL script (`d_ausd_bp_ta_cntrct_dist.sql`).

The job has been migrated to Google Cloud Platform, specifically targeting:
*   **BigQuery Stored Procedure:** The core orchestration logic, parameter handling, and the translated SQL processing from `d_ausd_bp_ta_cntrct_dist.sql` are now encapsulated within a BigQuery Stored Procedure named `project.dataset.r_ausd_bp_ta_cntrct_dist`.
*   **BigQuery Tables:** The source and target tables (`sof_ta_bpr_basis`, `sof_ta_cntrct_dist`) are now BigQuery tables. An optional `job_log_table` has been provided for logging.
*   **Python Orchestrator:** A Python script (`invoke_r_ausd_bp_ta_cntrct_dist.py`) is provided as an example to invoke the BigQuery Stored Procedure, replacing the shell script's role as the entry point.

## 2. Generated Artifacts

The following artifacts were generated as part of this migration:

*   **`project/dataset/r_ausd_bp_ta_cntrct_dist.sql`**
    *   **Role:** This file defines the BigQuery Stored Procedure `r_ausd_bp_ta_cntrct_dist`. It contains the translated logic from the original `k_ausd_bp_ta_cntrct_dist.ksh` script, including parameter validation, date parsing, and the core data processing SQL logic derived from `d_ausd_bp_ta_cntrct_dist.sql`. It replaces the shell script's orchestration and the embedded SQL execution.

*   **`project/dataset/ddl/sof_ta_cntrct_dist.sql`**
    *   **Role:** This file contains the Data Definition Language (DDL) for the target BigQuery table `sof_ta_cntrct_dist`. This table is where the processed contract distribution data will be stored, replacing its Oracle equivalent.

*   **`project/dataset/ddl/sof_ta_bpr_basis.sql`**
    *   **Role:** This file contains a placeholder DDL for the source BigQuery table `sof_ta_bpr_basis`. This table is expected to hold the input data for the processing, replacing its Oracle equivalent. Its schema should be updated to accurately reflect the full source table structure.

*   **`project/dataset/ddl/job_log_table.sql`**
    *   **Role:** This file provides an optional DDL for a BigQuery table intended for job logging. It serves as a BigQuery-native replacement for the commented-out `FOSJobErzeugeEintrag` functionality in the original KornShell script, allowing for centralized tracking of job execution status and metrics.

*   **`invoke_r_ausd_bp_ta_cntrct_dist.py`**
    *   **Role:** This Python script acts as an example external orchestrator. It demonstrates how to invoke the `r_ausd_bp_ta_cntrct_dist` BigQuery Stored Procedure, passing the necessary parameters. This script replaces the direct command-line execution of the original KornShell script and can be integrated into GCP scheduling services like Cloud Scheduler or Cloud Composer.

## 3. Key Design Decisions

The migration strategy focused on leveraging BigQuery's native capabilities to simplify the architecture and improve maintainability.

*   **Consolidation into BigQuery Stored Procedure:** The primary decision was to consolidate the KornShell orchestration logic and the core SQL processing into a single BigQuery Stored Procedure.
    *   **Why:** This centralizes the entire workflow within BigQuery, eliminating the need for external shell scripts, `sqlplus` calls, and temporary files. It leverages BigQuery's performance for data processing and its scripting capabilities for control flow, parameter validation, and error handling.
    *   **Trade-offs:** Debugging procedural logic within BigQuery Stored Procedures can sometimes be less intuitive than traditional scripting languages. It also increases reliance on BigQuery's specific SQL dialect and scripting features.

*   **Elimination of External Utilities and Temporary Files:**
    *   **Why:** The original script relied on various external shell utilities (`gestern.ksh`, `f_alis_msgerr.ksh`, `h_alis_date.ksh`, `h_alis_parameter.ksh`, `h_alis_sqlplus.ksh`) and temporary files for record counts. These have been replaced by BigQuery's built-in functions (e.g., `PARSE_DATE`, `CURRENT_DATE`, `COUNT(*)`) and scripting variables. This reduces external dependencies and simplifies deployment.
    *   **Trade-offs:** Requires careful translation of shell logic to BigQuery SQL/scripting, which might not always be a direct one-to-one mapping.

*   **Python Orchestrator for Invocation:**
    *   **Why:** While the core logic is in BigQuery, an external entry point is often required for scheduling and parameter management. A Python script provides a flexible, cloud-native way to invoke the BigQuery Stored Procedure, allowing for integration with GCP services like Cloud Scheduler or Cloud Composer.
    *   **Trade-offs:** Introduces a separate component to manage and deploy, though it's minimal and standard for GCP.

*   **Parameter Validation within Stored Procedure:**
    *   **Why:** Moving parameter validation (e.g., checking for missing parameters, date format) directly into the BigQuery Stored Procedure ensures that invalid inputs are caught early within the processing environment, providing immediate feedback and preventing unnecessary resource consumption.
    *   **Trade-offs:** Error messages are returned by BigQuery, which might require parsing by the invoking system.

## 4. Manual Steps Before Go-Live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset Creation:**
    *   Ensure the target BigQuery dataset (`project.dataset` in the generated code) exists. If not, create it in the GCP Console or using `bq mk`.
    *   `bq mk --dataset --default_location=US project:dataset` (adjust location as needed)

2.  **Source Data Ingestion:**
    *   The source table `project.dataset.sof_ta_bpr_basis` must be populated with data that corresponds to the original Oracle source. This data ingestion process (e.g., using Dataflow, BigQuery Data Transfer Service, or Cloud Storage loads) must be established and verified.
    *   **Action:** Create the `project.dataset.sof_ta_bpr_basis` table using the provided DDL (`project/dataset/ddl/sof_ta_bpr_basis.sql`) and load the historical and incremental data.

3.  **Target Table DDL Deployment:**
    *   Deploy the DDL for the target table `project.dataset.sof_ta_cntrct_dist`.
    *   **Action:** Execute the SQL in `project/dataset/ddl/sof_ta_cntrct_dist.sql` in BigQuery.

4.  **Optional Job Logging Table Deployment:**
    *   If job logging functionality is desired (to replace `FOSJobErzeugeEintrag`), deploy the DDL for the `job_log_table`.
    *   **Action:** Execute the SQL in `project/dataset/ddl/job_log_table.sql` in BigQuery.

5.  **BigQuery Stored Procedure Deployment:**
    *   Deploy the BigQuery Stored Procedure.
    *   **Action:** Execute the SQL in `project/dataset/r_ausd_bp_ta_cntrct_dist.sql` in BigQuery.

6.  **IAM Permissions:**
    *   **Service Account:** Create or identify a service account that will be used to execute the Python orchestrator or any other scheduling mechanism.
    *   **Permissions:** Grant the service account the following BigQuery roles:
        *   `BigQuery Data Editor` on the `project.dataset` dataset (or specific tables if fine-grained control is needed) to allow the Stored Procedure to write to `sof_ta_cntrct_dist` and potentially `job_log_table`.
        *   `BigQuery Data Viewer` on the `project.dataset` dataset (or specific tables) to allow the Stored Procedure to read from `sof_ta_bpr_basis`.
        *   `BigQuery Job User` at the project level to allow the service account to run BigQuery jobs (including Stored Procedures).
    *   **Python Orchestrator:** Ensure the environment where `invoke_r_ausd_bp_ta_cntrct_dist.py` runs is authenticated to GCP (e.g., via Application Default Credentials, `gcloud auth application-default login`, or by setting `GOOGLE_APPLICATION_CREDENTIALS`).

7.  **Scheduling:**
    *   Configure a scheduling mechanism (e.g., Cloud Scheduler, Cloud Composer/Airflow, or a custom cron job on a VM) to invoke the `invoke_r_ausd_bp_ta_cntrct_dist.py` script with the appropriate parameters.

## 5. Known Gaps & Unresolved References

The following items are flagged for follow-up or represent known limitations/assumptions:

*   **Completeness of `d_ausd_bp_ta_cntrct_dist.sql` Translation:** The generated BigQuery Stored Procedure assumes a direct translation of the core SQL logic from `d_ausd_bp_ta_cntrct_dist.sql`. The actual content of this original SQL file was not provided.
    *   **Action Required:** A thorough review and potential refinement of the SQL within `r_ausd_bp_ta_cntrct_dist.sql` is necessary once the full `d_ausd_bp_ta_cntrct_dist.sql` content is available. This includes verifying data types, function equivalences, and any procedural constructs.
*   **Source Table DDL (`sof_ta_bpr_basis`):** The DDL provided for `sof_ta_bpr_basis` is a placeholder with only `CNTRCT_ID`.
    *   **Action Required:** The DDL must be updated to precisely match the schema of the original Oracle `sof_ta_bpr_basis` table, including all columns and their correct BigQuery data types.
*   **`dwtk_meldungen` Logic:** The original SQL script might have contained logic related to a `dwtk_meldungen` table for determining a `v_datum`. The migrated SP assumes `p_Stichtag` is the primary driving date.
    *   **Action Required:** If the `dwtk_meldungen` logic is critical for data filtering or business date determination, it needs to be explicitly incorporated into the BigQuery Stored Procedure.
*   **Commented-out Functionality:** The original KornShell script contained commented-out sections for `sed`, `sort`, `join`, `FOSJobDeaktivate`, and `FOSJobErzeugeEintrag`.
    *   **Action Required:** Confirm with business stakeholders if these functionalities are still required.
        *   If `FOSJobErzeugeEintrag` is needed, the `job_log_table` DDL and `INSERT` statements in the SP should be activated and configured.
        *   If `sed`/`sort`/`join` logic is required, it needs to be translated into BigQuery SQL or scripting.
*   **Error Handling Granularity:** The BigQuery Stored Procedure uses `RAISE USING MESSAGE` for parameter validation errors.
    *   **Action Required:** Review if more granular error codes or specific logging mechanisms are required for integration with existing error monitoring systems.

## 6. Validation

Validation of the migrated job involves executing the BigQuery Stored Procedure and verifying its output and behavior.

1.  **Prepare Test Data:**
    *   Ensure the `project.dataset.sof_ta_bpr_basis` table contains representative test data that covers various scenarios (e.g., valid contracts, edge cases, different `p_Stichtag` values).
    *   Record the expected output for `project.dataset.sof_ta_cntrct_dist` based on the test data and the original script's logic.

2.  **Execute the Job:**
    *   Use the `invoke_r_ausd_bp_ta_cntrct_dist.py` script to call the BigQuery Stored Procedure.
    *   **Example Command:**
        ```bash
        python invoke_r_ausd_bp_ta_cntrct_dist.py \
          --project_id your-gcp-project \
          --dataset_id your_bigquery_dataset \
          --job_kennung TEST_JOB_ID \
          --eintrags_nr 12345 \
          --stichtag 01012023 \
          --wiederanlauf_wert 0
        ```
    *   Test with various valid and invalid parameters (e.g., missing `p_Stichtag`, invalid `p_Stichtag` format) to verify error handling.

3.  **Verify "Passing" Criteria:**
    *   **Successful Execution:** The Python script should report successful execution of the Stored Procedure without any BigQuery errors.
    *   **Data Integrity:**
        *   Query `project.dataset.sof_ta_cntrct_dist` and verify that the `CNTRCT_ID` values match the expected output based on the source data and the logic of `d_ausd_bp_ta_cntrct_dist.sql`.
        *   Confirm the count of records in `sof_ta_cntrct_dist` matches the expected count.
    *   **Error Handling:** When invalid parameters are provided, the Stored Procedure should `RAISE USING MESSAGE` with the correct error message, and the Python script should report a failure.
    *   **Logging (Optional):** If the `job_log_table` is enabled, verify that entries are correctly inserted with the appropriate status (`RUNNING`, `SUCCESS`, `FAILED`) and processed record counts.
    *   **Performance:** Monitor the execution time and slot consumption of the BigQuery job to ensure it meets performance expectations.

## 7. Rollback Procedure

In case of issues or unexpected behavior after deployment, the following steps outline the rollback procedure to revert to the original KornShell script execution:

1.  **Stop New Executions:**
    *   Immediately disable or remove any scheduled invocations of the `invoke_r_ausd_bp_ta_cntrct_dist.py` script (e.g., from Cloud Scheduler, Cloud Composer).

2.  **Revert to Original Scheduling:**
    *   Re-enable the original scheduling mechanism for `k_ausd_bp_ta_cntrct_dist.ksh` on the legacy platform.

3.  **Clean Up BigQuery Artifacts (Optional but Recommended):**
    *   **Delete Stored Procedure:**
        ```sql
        DROP PROCEDURE IF EXISTS `project.dataset.r_ausd_bp_ta_cntrct_dist`;
        ```
    *   **Truncate/Delete Target Table Data:** If the `sof_ta_cntrct_dist` table was populated by the migrated job and its data is not needed or is incorrect, truncate or delete its contents. If the table was created solely for this migration, it can be dropped.
        ```sql
        TRUNCATE TABLE `project.dataset.sof_ta_cntrct_dist`;
        -- OR
        DROP TABLE IF EXISTS `project.dataset.sof_ta_cntrct_dist`;
        ```
    *   **Delete Job Log Table (if created):**
        ```sql
        DROP TABLE IF EXISTS `project.dataset.job_log_table`;
        ```
    *   **Note:** Do NOT drop `project.dataset.sof_ta_bpr_basis` unless it was exclusively created for this migration and contains no other valuable data. This table is a source for the job and might be used by other processes.

4.  **Verify Legacy System Functionality:**
    *   Confirm that the original `k_ausd_bp_ta_cntrct_dist.ksh` script is running as expected and producing correct output on the legacy platform.

This rollback procedure ensures a quick return to the previous stable state while cleaning up the newly deployed BigQuery resources.