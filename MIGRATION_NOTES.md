# MIGRATION_NOTES.md

## 1. Summary

This document details the migration of the KornShell script `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bcp_msisdn.ksh` and its associated Oracle SQL script `d_ausd_bp_ta_bcp_msisdn.sql`. The original job was responsible for orchestrating data processing related to `PoolBasisprodukt`, including parameter validation, date derivation, core SQL execution, and optional file-based post-processing.

The job has been migrated to Google BigQuery, with its orchestration handled by Google Cloud Composer (Apache Airflow). The KornShell logic has been re-implemented as a BigQuery Stored Procedure, and the core SQL logic has been translated to BigQuery Standard SQL. File-based post-processing, if reactivated, is also handled within BigQuery DML statements.

## 2. Generated artifacts

The migration produced the following artifacts:

*   **`sql/d_ausd_bp_ta_bcp_msisdn_bq.sql`**: This file represents the BigQuery-compatible version of the original `d_ausd_bp_ta_bcp_msisdn.sql`. It contains the core data transformation logic, including `TRUNCATE` and `INSERT` statements to populate the `sof_ta_bcp_msisdn` table. While provided as a separate file for clarity, its content is embedded directly within the BigQuery Stored Procedure for execution.
    *   **Role**: Defines the primary data manipulation (DML) logic for enriching data with MSISDNs of BCP contracts.

*   **`sql/r_ausd_bp_ta_bcp_msisdn.sql`**: This file defines the BigQuery Stored Procedure `r_ausd_bp_ta_bcp_msisdn`. It encapsulates the entire logic of the original `k_ausd_bp_ta_bcp_msisdn.ksh` script, including parameter validation, date derivation, execution of the core SQL logic (from `d_ausd_bp_ta_bcp_msisdn_bq.sql`), record counting, and the migrated file-based post-processing.
    *   **Role**: Serves as the central execution unit in BigQuery, orchestrating the data processing workflow.

*   **`dags/k_ausd_bp_ta_bcp_msisdn_dag.py`**: This is an Apache Airflow DAG (Directed Acyclic Graph) written in Python. It is responsible for scheduling and triggering the execution of the BigQuery Stored Procedure `r_ausd_bp_ta_bcp_msisdn` within the Google Cloud Composer environment. It passes the necessary parameters to the stored procedure.
    *   **Role**: Provides external orchestration, scheduling, and parameter management for the BigQuery job.

## 3. Key design decisions

*   **KornShell to BigQuery Stored Procedure:** The orchestration logic of the original KornShell script (parameter parsing, validation, conditional execution, logging) was migrated to a BigQuery Stored Procedure. This centralizes the job's logic within BigQuery, leveraging its native capabilities for control flow, error handling, and parameter management, leading to better performance and maintainability compared to external shell execution.
*   **Oracle SQL to BigQuery Standard SQL:** The core data transformation logic from `d_ausd_bp_ta_bcp_msisdn.sql` was translated to BigQuery Standard SQL. This involved replacing Oracle-specific syntax (e.g., `TRUNCATE TABLE ... REUSE STORAGE`, implicit joins, Oracle hints) with their BigQuery equivalents.
*   **Embedding Core SQL in Stored Procedure:** Instead of calling the `d_ausd_bp_ta_bcp_msisdn_bq.sql` as a separate script from the Stored Procedure, its content was embedded directly. This simplifies deployment, reduces overhead, and allows for seamless variable passing within the BigQuery environment.
*   **File-based Post-processing to BigQuery DML:** The commented-out `sed`, `sort`, and `join` commands in the original script, if reactivated, are now implemented as BigQuery DML statements using temporary tables and `JOIN` operations. This keeps all data processing within BigQuery, avoiding external file I/O and leveraging BigQuery's distributed processing power. The interpretation of `join -a` flags as `RIGHT JOIN` and `LEFT JOIN` was a key decision for accurate translation.
*   **Airflow for Orchestration:** Google Cloud Composer (Apache Airflow) was chosen for external orchestration due to its robust scheduling capabilities, monitoring features, and native integration with Google Cloud services, providing a standard and scalable solution for managing data pipelines.
*   **BigQuery-native Date Handling:** Shell script date utilities (`gestern.ksh`) and validation functions (`DWDate_Datum_Check`) were replaced by BigQuery's built-in date functions (`CURRENT_DATE()`, `DATE_SUB()`, `SAFE.PARSE_DATE()`) for efficiency and consistency.
*   **Record Counting:** The original method of writing record counts to a temporary file and then reading it was replaced by a direct `SELECT COUNT(*)` on the target BigQuery table, storing the result in a `DECLARE`d variable within the Stored Procedure.

## 4. Manual steps before go-live

Before the migrated job can go live, the following manual steps must be completed:

1.  **BigQuery Dataset and Table Creation:**
    *   Ensure the target BigQuery dataset (`my_dataset`) exists.
    *   Create the following BigQuery tables with appropriate schemas:
        *   `my-gcp-project.my_dataset.sof_ta_bpr_bcp` (Source table)
        *   `my-gcp-project.my_dataset.sof_ta_rn_vertrag` (Source table)
        *   `my-gcp-project.my_dataset.sof_ta_bcp_msisdn` (Target table for core logic)
        *   `my-gcp-project.my_dataset.cibasisprodukt_csv` (Target table for post-processing output)
    *   **Note:** The schemas for `sof_ta_bpr_bcp` and `sof_ta_rn_vertrag` must match the columns referenced in the `INSERT` statement (`cntrct_id`, `bpr_id`, `cntrct_id_ref`, `tn_tel_msisdn`). The schema for `cibasisprodukt_csv` should accommodate the four output columns from the post-processing logic.

2.  **IAM Permissions:**
    *   The service account used by the Cloud Composer environment (or the user executing the job) must have sufficient BigQuery permissions:
        *   `bigquery.datasets.get`, `bigquery.tables.get`, `bigquery.tables.list` on `my_dataset`.
        *   `bigquery.tables.updateData`, `bigquery.tables.truncate`, `bigquery.tables.create` on `sof_ta_bcp_msisdn` and `cibasisprodukt_csv`.
        *   `bigquery.routines.create`, `bigquery.routines.update`, `bigquery.routines.get` for deploying and executing the Stored Procedure.
        *   `bigquery.jobs.create` to run BigQuery queries and procedures.

3.  **BigQuery Stored Procedure Deployment:**
    *   Execute the SQL script `sql/r_ausd_bp_ta_bcp_msisdn.sql` in your BigQuery environment to create or replace the stored procedure.
    *   **Important:** Replace `my-gcp-project` and `my_dataset` placeholders with your actual GCP project ID and dataset ID before deployment.

4.  **Airflow Connection Configuration:**
    *   Ensure the `google_cloud_default` connection is properly configured in your Airflow environment, pointing to the correct GCP project.

5.  **Airflow DAG Deployment:**
    *   Upload the `dags/k_ausd_bp_ta_bcp_msisdn_dag.py` file to your Cloud Composer environment's DAGs folder.
    *   **Important:**
        *   Update `PROJECT_ID` and `DATASET_ID` variables in the DAG to match your environment.
        *   Configure the `STICHTAG` parameter. The example uses a fixed date; for production, consider using Airflow macros (e.g., `{{ ds_nodash[6:] + ds_nodash[4:6] + ds_nodash[0:4] }}` for `DDMMYYYY` format from execution date) or Airflow variables for dynamic dates.
        *   Define the `schedule` for the DAG (e.g., `@daily`, `0 5 * * *`).

6.  **Initial Data Loading:**
    *   Ensure that the source tables (`sof_ta_bpr_bcp`, `sof_ta_rn_vertrag`) are populated with the necessary data before the first run of the migrated job.

## 5. Known gaps & unresolved references

*   **Exact Schema for Post-processing:** The generated BigQuery SQL for the post-processing step (creating `tmp_cibasis_data24_dat`, `tmp_cibasis_data96_dat`, `tmp_cibasis_fax_dat`, and `cibasisprodukt_csv`) makes assumptions about the column names (`col1`, `col2_bp`, `col3_msisdn`, `col2_ref`, `col2_fax`) and their derivation from the `sof_ta_bcp_msisdn` table. These assumptions need to be rigorously validated against the actual data structure and the intended logic of the original `sed`, `sort`, and `join` commands.
*   **Commented Job Tracking Reactivation:** The original KornShell script contained commented-out calls to `FOSJobDeaktivate` and `FOSJobErzeugeEintrag`. The BigQuery Stored Procedure includes a commented-out section for job tracking. A decision is required on whether this functionality needs to be reactivated in BigQuery, and if so, the `job_tracking_table` schema and insertion logic must be defined and implemented.
*   **Full Functionality of Sourced KornShell Scripts:** The design document flagged understanding the full functionality of `$HOME/.dw_init` and other sourced `.ksh` scripts. While the migration assumes their core logic (parameter handling, date utilities) is covered by BigQuery-native constructs, any subtle environment setups or specific utility functions not explicitly translated might represent a gap.
*   **Error Handling Parity:** The BigQuery Stored Procedure uses `RAISE USING MESSAGE` for error conditions. While this provides basic error reporting, it might not fully replicate the specific error concepts and reporting mechanisms provided by the original `f_alis_msgerr.ksh` script. Further analysis may be needed to ensure equivalent error logging and alerting.
*   **Airflow `STICHTAG` Parameter Configuration:** The provided Airflow DAG has a fixed `STICHTAG` value for demonstration. For production, this parameter needs to be dynamically configured, typically using Airflow macros or variables, to reflect the desired processing date (e.g., current date, previous day).
*   **Airflow DAG Schedule:** The `schedule` parameter in the Airflow DAG is currently `None`. It must be explicitly defined to match the required execution frequency of the job.

## 6. Validation

To ensure the migrated job functions correctly and produces accurate results, follow these validation steps:

1.  **Unit Testing (BigQuery Stored Procedure):**
    *   **Execution:** Manually execute the `r_ausd_bp_ta_bcp_msisdn` stored procedure in BigQuery with various sets of parameters (valid, invalid `Stichtag`, missing required parameters).
    *   **Passing Criteria:**
        *   The procedure should complete successfully for valid inputs.
        *   It should `RAISE` appropriate error messages for invalid or missing parameters.
        *   The `sof_ta_bcp_msisdn` table should be populated with data.
        *   The `cibasisprodukt_csv` table should be populated with post-processed data (if post-processing is active).
        *   The `log_message` outputs should correctly reflect the parameters and processing status.

2.  **Data Validation:**
    *   **Record Count:** After a successful run, verify that the `v_records` count reported by the stored procedure (and logged) matches the actual `COUNT(*)` from the `sof_ta_bcp_msisdn` table.
    *   **Output Comparison (Core Logic):** Compare the data in the `my-gcp-project.my_dataset.sof_ta_bcp_msisdn` table with the output generated by the original `d_ausd_bp_ta_bcp_msisdn.sql` script (or its equivalent output from the original KSH job). Ensure data integrity, column values, and row counts match.
    *   **Output Comparison (Post-processing):** If the post-processing logic is active, compare the data in `my-gcp-project.my_dataset.cibasisprodukt_csv` with the `cibasisprodukt.csv` file generated by the original KSH script. Pay close attention to column order, delimiters, and data values.

3.  **End-to-End Testing (Airflow DAG):**
    *   **Deployment:** Ensure the `k_ausd_bp_ta_bcp_msisdn_dag.py` DAG is deployed to Cloud Composer and appears in the Airflow UI.
    *   **Triggering:** Manually trigger the DAG from the Airflow UI.
    *   **Passing Criteria:**
        *   The DAG run should complete successfully (all tasks turn green).
        *   The `call_r_ausd_bp_ta_bcp_msisdn_procedure` task should show successful execution in its logs.
        *   Verify that the BigQuery Stored Procedure was called with the correct parameters as defined in the DAG.
        *   Confirm that the target BigQuery tables (`sof_ta_bcp_msisdn`, `cibasisprodukt_csv`) are updated as expected, matching the data validation criteria above.

## 7. Rollback procedure

In case of issues or unexpected behavior after deployment, the following rollback procedure can be executed:

1.  **Pause Airflow DAG:**
    *   In the Airflow UI, locate the `k_ausd_bp_ta_bcp_msisdn_bq` DAG and toggle its status to "Off" (paused). This will prevent any further scheduled executions.

2.  **Revert BigQuery Stored Procedure (Optional):**
    *   If a previous version of the `r_ausd_bp_ta_bcp_msisdn` stored procedure exists and is required, deploy that version. Otherwise, the current procedure can be left as is, as pausing the DAG prevents its execution.
    *   To completely remove the new procedure: `DROP PROCEDURE IF EXISTS \`my-gcp-project.my_dataset.r_ausd_bp_ta_bcp_msisdn\`;`

3.  **Restore Data (if necessary):**
    *   If the `sof_ta_bcp_msisdn` or `cibasisprodukt_csv` tables were corrupted or incorrectly updated by the migrated job, restore them from the most recent valid backup.
    *   Alternatively, if the original job is still functional, re-run the original `k_ausd_bp_ta_bcp_msisdn.ksh` script to overwrite the BigQuery tables with correct data (assuming the original job also writes to BigQuery or a system that can be loaded into BigQuery).

4.  **Re-enable Original Job:**
    *   Ensure the original `vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bcp_msisdn.ksh` script is fully functional and can be re-enabled in its original environment.
    *   Resume its scheduling and execution as per the pre-migration setup.

5.  **Monitor:**
    *   Closely monitor the re-enabled original job and the BigQuery environment to ensure stability and data integrity.